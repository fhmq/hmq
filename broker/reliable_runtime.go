package broker

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/fhmq/hmq/v4/broker/cluster"
	"github.com/fhmq/hmq/v4/broker/eventstore"
	"github.com/fhmq/hmq/v4/broker/reliable"
	"github.com/fhmq/hmq/v4/broker/reliable/inbox"
)

// This protocol is experimental: receipt is local and processing is at least
// once. It provides neither consensus nor replicated storage.
var reliablePrefix = []byte("HMQ-RELIABLE-1\x00")

type runtimeNodeSender struct {
	mu   sync.RWMutex
	node *cluster.Node
}

func (s *runtimeNodeSender) set(n *cluster.Node) { s.mu.Lock(); s.node = n; s.mu.Unlock() }
func (s *runtimeNodeSender) Send(peer string, data []byte) error {
	s.mu.RLock()
	n := s.node
	s.mu.RUnlock()
	if n == nil {
		return errors.New("reliable runtime: cluster not ready")
	}
	return n.Send(peer, data)
}

type reliableSender struct{ sender reliable.Sender }

func (s reliableSender) Send(peer string, data []byte) error {
	frame := make([]byte, 0, len(reliablePrefix)+len(data))
	frame = append(frame, reliablePrefix...)
	frame = append(frame, data...)
	return s.sender.Send(peer, frame)
}

type reliableRuntime struct {
	mu        sync.RWMutex
	closed    bool
	once      sync.Once
	events    *eventstore.Store
	inbox     *inbox.Store
	fanout    *fanoutStore
	links     map[string]*reliable.Link
	consumers []*inbox.Consumer
	closeErr  error
}

// reliableHealth intentionally reports counts rather than error text, which
// can contain local paths or message contents.
type reliableHealth struct {
	State                string `json:"state"`
	FailedLinks          int    `json:"failedLinks"`
	FailedInboxConsumers int    `json:"failedInboxConsumers"`
}

func (r *reliableRuntime) health() reliableHealth {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h := reliableHealth{State: "running"}
	if r.closed {
		h.State = "closed"
		return h
	}
	for _, link := range r.links {
		if link.Err() != nil {
			h.FailedLinks++
		}
	}
	for _, consumer := range r.consumers {
		if consumer.Err() != nil {
			h.FailedInboxConsumers++
		}
	}
	if h.FailedLinks != 0 || h.FailedInboxConsumers != 0 {
		h.State = "degraded"
	}
	return h
}

// The sender must be ready before callbacks are dispatched. The caller must
// authenticate peer IDs and route frames here before trying other protocols.
func newReliableRuntime(local string, members []string, eventPath, inboxPath string, sender reliable.Sender, interval time.Duration, maxFrame uint32, deliver func(inbox.Message) error) (*reliableRuntime, error) {
	return newBoundReliableRuntime(local, members, eventPath, inboxPath, sender, interval, maxFrame, deliver, nil)
}

func newBoundReliableRuntime(local string, members []string, eventPath, inboxPath string, sender reliable.Sender, interval time.Duration, maxFrame uint32, deliver func(inbox.Message) error, identity []byte) (*reliableRuntime, error) {
	if sender == nil || deliver == nil || interval <= 0 || local == "" || eventPath == "" || inboxPath == "" || eventPath == inboxPath {
		return nil, errors.New("reliable runtime: invalid configuration")
	}
	existing := false
	if identity != nil {
		state := func(path string) (bool, error) {
			s, err := os.Stat(path)
			if errors.Is(err, os.ErrNotExist) {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			return s.Size() != 0, nil
		}
		a, err := state(eventPath)
		if err != nil {
			return nil, err
		}
		b, err := state(inboxPath)
		if err != nil {
			return nil, err
		}
		if a != b {
			return nil, errors.New("reliable runtime: incomplete log pair")
		}
		existing = a
	}
	if maxFrame == 0 {
		maxFrame = reliable.DefaultMaxFrameSize
	}
	if maxFrame <= uint32(len(reliablePrefix)) {
		return nil, reliable.ErrFrameTooLarge
	}
	maxFrame -= uint32(len(reliablePrefix))
	seen := map[string]bool{}
	var peers []string
	for _, id := range members {
		if !clusterNodeIDPattern.MatchString(id) || seen[id] {
			return nil, errors.New("reliable runtime: invalid membership")
		}
		seen[id] = true
		if id != local {
			peers = append(peers, id)
		}
	}
	if !seen[local] || len(peers) == 0 {
		return nil, errors.New("reliable runtime: invalid membership")
	}
	r := &reliableRuntime{links: make(map[string]*reliable.Link)}
	var err error
	r.events, err = eventstore.Open(eventPath, eventstore.Limits{})
	if err != nil {
		return nil, err
	}
	ready := false
	defer func() {
		if !ready {
			r.Close()
		}
	}()
	r.inbox, err = inbox.Open(inboxPath, eventstore.Limits{})
	if err != nil {
		return nil, err
	}
	if identity != nil {
		binding := append(append([]byte(nil), identity...), 0)
		if existing {
			var event eventstore.Event
			event, err = r.events.Get("!hmq-log-identity-v1")
			if err != nil {
				return nil, err
			}
			var incoming []byte
			incoming, err = r.inbox.Identity()
			if err != nil {
				return nil, err
			}
			if !bytes.HasPrefix(event.Payload, append([]byte("events:"), binding...)) || len(event.Payload) != len("events:")+len(binding)+32 || !bytes.Equal(event.Payload[len("events:"):], bytes.TrimPrefix(incoming, []byte("inbox:"))) || !bytes.HasPrefix(incoming, []byte("inbox:")) {
				return nil, errors.New("reliable runtime: log identity mismatch")
			}
			binding = event.Payload[len("events:"):]
		} else {
			var nonce [32]byte
			if _, err = rand.Read(nonce[:]); err != nil {
				return nil, err
			}
			binding = append(binding, nonce[:]...)
		}
		err = r.events.BindIdentity(append([]byte("events:"), binding...))
		if err != nil {
			return nil, err
		}
		err = r.inbox.BindIdentity(append([]byte("inbox:"), binding...))
		if err != nil {
			return nil, err
		}
	}
	r.fanout, err = newFanoutStore(r.events, peers, maxFrame)
	if err != nil {
		return nil, err
	}
	for _, peer := range peers {
		var view reliable.Store
		view, err = r.fanout.view(peer)
		if err != nil {
			return nil, err
		}
		var link *reliable.Link
		link, err = reliable.NewWithMaxFrameSize(peer, reliableSender{sender}, view, interval, maxFrame)
		if err != nil {
			return nil, err
		}
		r.links[peer] = link
		var consumer *inbox.Consumer
		consumer, err = inbox.NewConsumer(r.inbox, peer, interval, deliver)
		if err != nil {
			return nil, err
		}
		r.consumers = append(r.consumers, consumer)
	}
	ready = true
	return r, nil
}

// enqueue commits the complete fanout before scheduling any link. A failure
// after the commit has an uncertain outcome and must not be treated as rollback.
func (r *reliableRuntime) enqueue(payload []byte) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return reliable.ErrClosed
	}
	for _, link := range r.links {
		if err := link.Err(); err != nil {
			return err
		}
	}
	var id [32]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	key := hex.EncodeToString(id[:])
	if err := r.fanout.enqueue(key, payload); err != nil {
		return err
	}
	for _, link := range r.links {
		if err := link.Resume(key); err != nil {
			return err
		}
	}
	return nil
}

// handle returns false only for a non-reliable protocol frame. Malformed or
// unconfigured reliable frames must never fall through to another protocol.
func (r *reliableRuntime) handle(peer string, data []byte) (bool, error) {
	if !bytes.HasPrefix(data, reliablePrefix) {
		return false, nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return true, reliable.ErrClosed
	}
	link := r.links[peer]
	if link == nil {
		return true, reliable.ErrPeer
	}
	return true, link.HandleInbox(peer, data[len(reliablePrefix):], r.inbox)
}

func (r *reliableRuntime) Close() error {
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()
		for _, c := range r.consumers {
			c.Stop()
		}
		for _, l := range r.links {
			l.Close()
		}
		for _, c := range r.consumers {
			c.Close()
		}
		if r.inbox != nil {
			r.closeErr = errors.Join(r.closeErr, r.inbox.Close())
		}
		if r.events != nil {
			r.closeErr = errors.Join(r.closeErr, r.events.Close())
		}
	})
	return r.closeErr
}
