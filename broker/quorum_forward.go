package broker

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fhmq/hmq/v4/broker/packets"
	"github.com/fhmq/hmq/v4/broker/replication"
	"github.com/fhmq/hmq/v4/broker/storage"
	"github.com/hashicorp/raft"
)

const (
	quorumForwardDomain    = "hmq.quorum.forward.v1"
	quorumForwardBytes     = 8 << 20
	quorumForwardFrame     = 1 << 20
	quorumForwardWorkers   = 8
	quorumForwardPeerLimit = 16
)

type quorumForwardFrameData struct {
	Domain      string
	Response    bool
	Operation   string
	Attempt     uint32
	Digest      string
	Payload     []byte `json:",omitempty"`
	BudgetNanos int64
	Index       uint64
	Error       quorumForwardError `json:",omitempty"`
}

type quorumForwardError string

const (
	quorumForwardCapacity quorumForwardError = "capacity"
	quorumForwardConflict quorumForwardError = "conflict"
	quorumForwardInvalid  quorumForwardError = "invalid"
)

func quorumForwardTerminalError(err error) quorumForwardError {
	switch {
	case errors.Is(err, replication.ErrCapacity):
		return quorumForwardCapacity
	case errors.Is(err, replication.ErrConflict):
		return quorumForwardConflict
	case errors.Is(err, replication.ErrInvalid):
		return quorumForwardInvalid
	default:
		return ""
	}
}

func (e quorumForwardError) err() error {
	switch e {
	case quorumForwardCapacity:
		return replication.ErrCapacity
	case quorumForwardConflict:
		return replication.ErrConflict
	case quorumForwardInvalid:
		return replication.ErrInvalid
	default:
		return nil
	}
}

type quorumForwardResult struct {
	index   uint64
	err     error
	attempt uint32
}

type quorumPending struct {
	digest   string
	attempts map[uint32]string
	latest   uint32
	result   chan quorumForwardResult
}
type quorumWork struct {
	peer     string
	frame    quorumForwardFrameData
	size     int
	deadline time.Time
}
type quorumForward struct {
	r                   *quorumRuntime
	id, boot            string
	members             map[string]string
	send                func(context.Context, string, []byte) error
	ctx                 context.Context
	cancel              context.CancelFunc
	seq                 atomic.Uint64
	mu                  sync.Mutex
	closed              bool
	pending             map[string]*quorumPending
	bytes, workBytes    int
	peers               map[string]int
	requests, responses chan quorumWork
	wg                  sync.WaitGroup
}

func newQuorumForward(r *quorumRuntime, id string, members map[string]string, send func(context.Context, string, []byte) error) (*quorumForward, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("quorum forwarding nonce: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	f := &quorumForward{r: r, id: id, boot: hex.EncodeToString(nonce[:]), members: members, send: send, ctx: ctx, cancel: cancel, pending: make(map[string]*quorumPending), peers: make(map[string]int), requests: make(chan quorumWork, 64), responses: make(chan quorumWork, 64)}
	for i := 0; i < quorumForwardWorkers; i++ {
		f.wg.Add(2)
		go f.worker(false)
		go f.worker(true)
	}
	return f, nil
}

func (f *quorumForward) BeginClose() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.closed {
		f.closed = true
		f.cancel()
	}
}

func quorumDigest(data []byte) string { d := sha256.Sum256(data); return hex.EncodeToString(d[:]) }

func quorumForwardContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func (f *quorumForward) publish(data []byte) error {
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	defer cancel()
	op := fmt.Sprintf("%s:%s:%d", f.id, f.boot, f.seq.Add(1))
	deadline, _ := ctx.Deadline()
	frame := quorumForwardFrameData{Domain: quorumForwardDomain, Operation: op, Digest: quorumDigest(data), Payload: data, BudgetNanos: int64(5 * time.Second)}
	wire, _ := stdjson.Marshal(frame)
	if len(wire) > quorumForwardFrame {
		return replication.ErrInvalid
	}
	p := &quorumPending{digest: frame.Digest, attempts: make(map[uint32]string), result: make(chan quorumForwardResult, 1)}
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return storage.ErrClosed
	}
	if len(f.pending) >= quorumProposalLimit || f.bytes+len(data) > quorumForwardBytes {
		f.mu.Unlock()
		return replication.ErrBusy
	}
	f.pending[op] = p
	f.bytes += len(data)
	f.mu.Unlock()
	defer func() { f.mu.Lock(); delete(f.pending, op); f.bytes -= len(data); f.mu.Unlock() }()
	tick := time.NewTicker(150 * time.Millisecond)
	defer tick.Stop()
	for attempt := uint32(1); ; attempt++ {
		if err := quorumForwardContextError(ctx); err != nil {
			return err
		}
		_, leader := f.r.node.Leader()
		target := string(leader)
		if f.r.node.State() == raft.Leader {
			target = f.id
		}
		if _, ok := f.members[target]; ok {
			frame.Attempt = attempt
			f.mu.Lock()
			p.attempts[attempt] = target
			p.latest = attempt
			f.mu.Unlock()
			if target == f.id {
				remaining := time.Until(deadline)
				if remaining <= 0 {
					return context.DeadlineExceeded
				}
				index, err := f.r.node.Publish(op, data, remaining)
				if quorumForwardTerminalError(err) != "" {
					return err
				}
				if err == nil && index != 0 && quorumForwardContextError(ctx) == nil {
					return nil
				}
			} else {
				frame.BudgetNanos = int64(time.Until(deadline))
				if frame.BudgetNanos <= 0 {
					return context.DeadlineExceeded
				}
				wire, _ = stdjson.Marshal(frame)
				if len(wire) > quorumForwardFrame {
					return replication.ErrInvalid
				}
				_ = f.send(ctx, target, wire)
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case result := <-p.result:
			if err := quorumForwardContextError(ctx); err != nil {
				return err
			}
			if result.err != nil {
				f.mu.Lock()
				stale := result.attempt != p.latest
				f.mu.Unlock()
				if stale {
					continue
				}
			}
			return result.err
		case <-tick.C:
		}
	}
}

func (f *quorumForward) handle(peer string, data []byte) {
	received := time.Now()
	if len(data) > quorumForwardFrame {
		return
	}
	if _, ok := f.members[peer]; !ok {
		return
	}
	var m quorumForwardFrameData
	if stdjson.Unmarshal(data, &m) != nil || m.Domain != quorumForwardDomain || m.Attempt == 0 || len(m.Operation) > 200 || len(m.Digest) != 64 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return
	}
	if m.Response {
		p := f.pending[m.Operation]
		if p != nil && m.Digest == p.digest && p.attempts[m.Attempt] == peer && len(m.Payload) == 0 {
			result := quorumForwardResult{index: m.Index, attempt: m.Attempt}
			if m.Error == "" {
				if m.Index == 0 {
					return
				}
			} else {
				result.err = m.Error.err()
				if m.Index != 0 || result.err == nil || m.Attempt != p.latest {
					return
				}
			}
			select {
			case p.result <- result:
			default:
			}
		}
		return
	}
	if m.Error != "" || m.Index != 0 {
		return
	}
	parts := strings.Split(m.Operation, ":")
	if len(parts) != 3 || parts[0] != peer || len(parts[1]) != 32 || len(parts[2]) == 0 || len(parts[2]) > 20 {
		return
	}
	if _, err := hex.DecodeString(parts[1]); err != nil {
		return
	}
	for _, c := range parts[2] {
		if c < '0' || c > '9' {
			return
		}
	}
	remaining := time.Duration(m.BudgetNanos)
	if remaining > 5*time.Second {
		remaining = 5 * time.Second
	}
	if remaining <= 0 || len(m.Payload) > replication.MaxPayloadSize || quorumDigest(m.Payload) != m.Digest {
		return
	}
	var pub clusterPublish
	if stdjson.Unmarshal(m.Payload, &pub) != nil || !validQuorumPublication(&packets.PublishPacket{FixedHeader: packets.FixedHeader{Qos: pub.QoS, Retain: pub.Retain}, TopicName: pub.Topic, Payload: pub.Payload}) {
		return
	}
	if f.peers[peer] >= quorumForwardPeerLimit || f.workBytes+len(data) > quorumForwardBytes {
		return
	}
	select {
	case f.requests <- quorumWork{peer: peer, frame: m, size: len(data), deadline: received.Add(remaining)}:
		f.peers[peer]++
		f.workBytes += len(data)
	default:
	}
}

func (f *quorumForward) worker(response bool) {
	defer f.wg.Done()
	queue := f.requests
	if response {
		queue = f.responses
	}
	for {
		select {
		case <-f.ctx.Done():
			return
		case w := <-queue:
			if response {
				ctx, cancel := context.WithDeadline(f.ctx, w.deadline)
				wire, _ := stdjson.Marshal(w.frame)
				_ = f.send(ctx, w.peer, wire)
				cancel()
				continue
			}
			func() {
				defer func() { f.mu.Lock(); f.peers[w.peer]--; f.workBytes -= w.size; f.mu.Unlock() }()
				if f.ctx.Err() != nil || f.r.node.State() != raft.Leader {
					return
				}
				remaining := time.Until(w.deadline)
				if remaining <= 0 {
					return
				}
				if err := f.r.acquireProposal(); err != nil {
					return
				}
				defer f.r.releaseProposal()
				index, err := f.r.node.Publish(w.frame.Operation, w.frame.Payload, remaining)
				code := quorumForwardTerminalError(err)
				if (err != nil && code == "") || (err == nil && index == 0) {
					return
				}
				w.frame.Response = true
				w.frame.Payload = nil
				w.frame.Index = index
				w.frame.Error = code
				if code != "" {
					w.frame.Index = 0
				}
				select {
				case f.responses <- w:
				default:
				}
			}()
		}
	}
}
