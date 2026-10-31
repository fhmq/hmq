// Package reliable provides retrying, acknowledged delivery to one fixed peer.
package reliable

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/fhmq/hmq/v4/broker/eventstore"
)

var ErrClosed = errors.New("reliable: closed")
var ErrPeer = errors.New("reliable: unexpected peer")
var ErrFrame = errors.New("reliable: invalid frame")
var ErrUnknown = errors.New("reliable: unknown pending event")
var ErrConflict = errors.New("reliable: inbox identity conflict")
var ErrFrameTooLarge = errors.New("reliable: frame exceeds maximum size")

// DefaultMaxFrameSize matches cluster.Config's default MaxFrameSize.
const DefaultMaxFrameSize uint32 = 1024 * 1024

// Inbox atomically and durably records a received message before returning nil.
// The key is (peer, id). Repeated calls with identical payload must succeed;
// a different payload for an existing key must return ErrConflict. Implementations
// must serialize concurrent calls and preserve records across process restarts.
// A nil result means the message is recoverable for application processing,
// not that external side effects have completed.
type Inbox interface {
	Accept(peer, id string, payload []byte) error
}

// Sender is implemented by *cluster.Node. Send must eventually return.
type Sender interface {
	Send(peer string, payload []byte) error
}

// Store is implemented by *eventstore.Store. Calls must be concurrency-safe.
type Store interface {
	Put(id string, payload []byte, targets []string) error
	Get(id string) (eventstore.Event, error)
	Ack(id, target string) error
	Pending(target string, limit int) ([]eventstore.Event, error)
	PendingAfter(target string, cursor uint64, limit int) ([]eventstore.Event, uint64, error)
	PendingUntil(target string, cursor, through uint64, limit int) ([]eventstore.Event, uint64, error)
	Sequence() (uint64, error)
}

type frame struct {
	Version int    `json:"version"`
	Type    string `json:"type"`
	ID      string `json:"id"`
	Payload []byte `json:"payload,omitempty"`
}

// Link owns neither sender nor store. IDs must be unique across all uses of the
// store, including across restarts. A Link must be the sole owner of its IDs.
type Link struct {
	peer         string
	sender       Sender
	store        Store
	maxFrameSize uint32
	mu           sync.Mutex
	pending      map[string]struct{}
	cursor       uint64
	through      uint64
	cycleActive  bool
	closed       bool
	loadErr      error
	done         chan struct{}
	wake         chan struct{}
	wg           sync.WaitGroup
}

// New recovers a bounded batch of pending events and starts a retry worker.
// The store's pending events for this peer must belong exclusively to this
// link and have exactly one target. Batches rotate independently of ACKs.
// The receiver callback must route authenticated cluster frames to Handle.
func New(peer string, sender Sender, store Store, interval time.Duration) (*Link, error) {
	return NewWithMaxFrameSize(peer, sender, store, interval, DefaultMaxFrameSize)
}

// NewWithMaxFrameSize sets the maximum encoded payload passed to Sender.Send.
// Use the effective cluster.Config.MaxFrameSize; zero selects cluster's default.
func NewWithMaxFrameSize(peer string, sender Sender, store Store, interval time.Duration, maxFrameSize uint32) (*Link, error) {
	if peer == "" || sender == nil || store == nil || interval <= 0 {
		return nil, errors.New("reliable: invalid configuration")
	}
	if maxFrameSize == 0 {
		maxFrameSize = DefaultMaxFrameSize
	}
	if maxFrameSize == ^uint32(0) {
		return nil, errors.New("reliable: invalid maximum frame size")
	}
	l := &Link{peer: peer, sender: sender, store: store, maxFrameSize: maxFrameSize, pending: make(map[string]struct{}), done: make(chan struct{}), wake: make(chan struct{}, 1)}
	if err := l.loadPendingLocked(); err != nil {
		return nil, err
	}
	l.wg.Add(1)
	go l.run(interval)
	select {
	case l.wake <- struct{}{}:
	default:
	}
	return l, nil
}

const recoveryBatch = 256

// validUnicodeEscapes runs after JSON syntax validation, so every escape has
// enough bytes. encoding/json otherwise silently replaces unpaired surrogates.
func validUnicodeEscapes(data []byte) bool {
	inString := false
	for i := 0; i < len(data); i++ {
		if !inString {
			if data[i] == '"' {
				inString = true
			}
			continue
		}
		if data[i] == '"' {
			inString = false
			continue
		}
		if data[i] != '\\' {
			continue
		}
		i++
		if data[i] != 'u' {
			continue
		}
		v, _ := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		i += 4
		if v >= 0xdc00 && v <= 0xdfff {
			return false
		}
		if v >= 0xd800 && v <= 0xdbff {
			if i+6 >= len(data) || data[i+1] != '\\' || data[i+2] != 'u' {
				return false
			}
			low, err := strconv.ParseUint(string(data[i+3:i+7]), 16, 16)
			if err != nil || low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		}
	}
	return true
}

func (l *Link) loadPendingLocked() error {
	events, err := l.store.Pending(l.peer, recoveryBatch)
	if err != nil {
		return err
	}
	for _, e := range events {
		if _, exists := l.pending[e.ID]; !exists && len(l.pending) >= recoveryBatch {
			break
		}
		if len(e.Targets) != 1 || e.Targets[0] != l.peer {
			return ErrPeer
		}
		if err := l.validateData(e.ID, e.Payload); err != nil {
			return fmt.Errorf("reliable: event %q: %w", e.ID, err)
		}
		l.pending[e.ID] = struct{}{}
	}
	return nil
}

func (l *Link) rotatePendingLocked() error {
	cursor, through, active := l.cursor, l.through, l.cycleActive
	if !active {
		var err error
		through, err = l.store.Sequence()
		if err != nil {
			return err
		}
		cursor = 0
	}
	events, cursor, err := l.store.PendingUntil(l.peer, cursor, through, recoveryBatch)
	if err != nil {
		return err
	}
	if len(events) == 0 && active {
		through, err = l.store.Sequence()
		if err != nil {
			return err
		}
		events, cursor, err = l.store.PendingUntil(l.peer, 0, through, recoveryBatch)
		if err != nil {
			return err
		}
	}
	next := make(map[string]struct{}, len(events))
	for _, e := range events {
		if len(e.Targets) != 1 || e.Targets[0] != l.peer {
			return ErrPeer
		}
		if err := l.validateData(e.ID, e.Payload); err != nil {
			return fmt.Errorf("reliable: event %q: %w", e.ID, err)
		}
		next[e.ID] = struct{}{}
	}
	l.pending, l.cursor, l.through = next, cursor, through
	l.cycleActive = len(events) == recoveryBatch
	return nil
}

// Enqueue durably records the payload for the fixed peer before scheduling it.
func (l *Link) Enqueue(id string, payload []byte) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if err := l.validateData(id, payload); err != nil {
		return err
	}
	if err := l.store.Put(id, payload, []string{l.peer}); err != nil {
		return err
	}
	return l.resumeLocked(id)
}

func (l *Link) validateData(id string, payload []byte) error {
	if id == "" || !utf8.ValidString(id) {
		return ErrFrame
	}
	b, err := json.Marshal(frame{Version: 1, Type: "data", ID: id, Payload: payload})
	if err != nil {
		return err
	}
	if uint64(len(b)) > uint64(l.maxFrameSize) {
		return ErrFrameTooLarge
	}
	return nil
}

// Resume validates a previously persisted ID and wakes the retry worker.
// The ID may wait until its durable batch is reached.
func (l *Link) Resume(id string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	return l.resumeLocked(id)
}

func (l *Link) resumeLocked(id string) error {
	e, err := l.store.Get(id)
	if err != nil {
		return err
	}
	if len(e.Targets) != 1 || e.Targets[0] != l.peer {
		return ErrPeer
	}
	for _, p := range e.Confirmed {
		if p == l.peer {
			return nil
		}
	}
	if err := l.validateData(e.ID, e.Payload); err != nil {
		return fmt.Errorf("reliable: event %q: %w", e.ID, err)
	}
	if len(l.pending) < recoveryBatch {
		if err := l.loadPendingLocked(); err != nil {
			return err
		}
	}
	select {
	case l.wake <- struct{}{}:
	default:
	}
	return nil
}

// Handle processes a frame received from the authenticated peer. deliver must
// return nil only after the receiver's required durable work has completed.
// It may be invoked repeatedly or concurrently for the same ID. ACK send
// failure is returned; the sender will retry the data frame.
func (l *Link) Handle(peer string, data []byte, deliver func(id string, payload []byte) error) error {
	if peer != l.peer {
		return ErrPeer
	}
	var f frame
	if !utf8.Valid(data) || !json.Valid(data) || !validUnicodeEscapes(data) {
		return ErrFrame
	}
	if err := json.Unmarshal(data, &f); err != nil || f.Version != 1 || f.ID == "" {
		return ErrFrame
	}
	switch f.Type {
	case "ack":
		if f.Payload != nil {
			return ErrFrame
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.closed {
			return ErrClosed
		}
		if _, ok := l.pending[f.ID]; !ok {
			e, err := l.store.Get(f.ID)
			if errors.Is(err, eventstore.ErrNotFound) {
				return ErrUnknown
			}
			if err != nil {
				return err
			}
			if len(e.Targets) != 1 || e.Targets[0] != l.peer {
				return ErrUnknown
			}
		}
		if err := l.store.Ack(f.ID, l.peer); err != nil {
			return err
		}
		delete(l.pending, f.ID)
		select {
		case l.wake <- struct{}{}:
		default:
		}
		return nil
	case "data":
		l.mu.Lock()
		closed := l.closed
		l.mu.Unlock()
		if closed {
			return ErrClosed
		}
		if deliver == nil {
			return ErrFrame
		}
		if err := deliver(f.ID, f.Payload); err != nil {
			return err
		}
		b, err := json.Marshal(frame{Version: 1, Type: "ack", ID: f.ID})
		if err != nil {
			return err
		}
		return l.sender.Send(l.peer, b)
	default:
		return ErrFrame
	}
}

// HandleInbox acknowledges data only after the inbox has durably accepted it.
// ACK frames use the same sender-side processing as Handle. The application
// owns inbox consumption and recovery; external effects remain at-least-once.
func (l *Link) HandleInbox(peer string, data []byte, inbox Inbox) error {
	if inbox == nil {
		return ErrFrame
	}
	return l.Handle(peer, data, func(id string, payload []byte) error {
		return inbox.Accept(peer, id, payload)
	})
}

func (l *Link) run(interval time.Duration) {
	defer l.wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-l.done:
			return
		case <-t.C:
		case <-l.wake:
		}
		l.mu.Lock()
		if l.loadErr != nil {
			l.mu.Unlock()
			continue
		}
		if err := l.rotatePendingLocked(); err != nil {
			if errors.Is(err, ErrFrameTooLarge) || errors.Is(err, ErrFrame) || errors.Is(err, ErrPeer) {
				l.loadErr = err
			}
			l.mu.Unlock()
			continue
		}
		ids := make([]string, 0, len(l.pending))
		for id := range l.pending {
			ids = append(ids, id)
		}
		l.mu.Unlock()
		for _, id := range ids {
			select {
			case <-l.done:
				return
			default:
			}
			l.mu.Lock()
			_, pending := l.pending[id]
			if !pending || l.closed {
				l.mu.Unlock()
				continue
			}
			e, err := l.store.Get(id)
			l.mu.Unlock()
			if err != nil {
				continue
			}
			b, err := json.Marshal(frame{Version: 1, Type: "data", ID: id, Payload: e.Payload})
			if err == nil {
				_ = l.sender.Send(l.peer, b)
			}
		}
	}
}

// Err reports a permanent error discovered while recovering a later batch.
// A non-nil result stops background delivery; reconstruct the link after
// correcting its configuration or persisted records.
func (l *Link) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.loadErr
}

// Close stops and joins the retry worker. It does not wait for Handle calls
// already in progress, and does not close the sender or store.
func (l *Link) Close() error {
	l.mu.Lock()
	if !l.closed {
		l.closed = true
		close(l.done)
	}
	l.mu.Unlock()
	l.wg.Wait()
	return nil
}

// Pending reports whether an ID is scheduled in this process.
func (l *Link) Pending(id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.pending[id]
	return ok
}
