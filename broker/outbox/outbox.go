// Package outbox implements a durable, single-peer pending-message queue.
// It provides neither replication nor exactly-once delivery. The WAL grows
// until Compact is called; capacity limits pending messages and payload bytes.
package outbox

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"unicode/utf8"

	"github.com/fhmq/hmq/v3/broker/storage"
)

var ErrFull = errors.New("outbox: capacity exceeded")
var ErrClosed = errors.New("outbox: closed")
var ErrNotFound = errors.New("outbox: message not found")
var ErrInvalid = errors.New("outbox: invalid argument or log")

type Message struct {
	ID      uint64
	Payload []byte
}
type record struct {
	Type        string `json:"type"`
	Peer        string `json:"peer,omitempty"`
	Version     int    `json:"version,omitempty"`
	Incarnation string `json:"incarnation,omitempty"`
	ID          uint64 `json:"id,omitempty"`
	Payload     []byte `json:"payload,omitempty"`
}
type log interface {
	Append([]byte) error
	Replay(func([]byte) error) error
	Close() error
}
type Outbox struct {
	mu                    sync.Mutex
	log                   log
	peer                  string
	incarnation           string
	maxMessages, maxBytes int
	bytes                 int
	next                  uint64
	pending               map[uint64][]byte
	order                 []uint64
	closed                bool
	failed                error
}

// Open opens a WAL dedicated to peer. Limits must be positive and must cover
// the recovered pending set. The parent directory must already exist.
func Open(path, peer string, maxMessages, maxBytes int) (*Outbox, error) {
	if peer == "" || !utf8.ValidString(peer) || maxMessages <= 0 || maxBytes <= 0 {
		return nil, ErrInvalid
	}
	l, err := storage.Open(path)
	if err != nil {
		return nil, err
	}
	o, err := recoverLog(l, peer, maxMessages, maxBytes)
	if err != nil {
		l.Close()
		return nil, err
	}
	return o, nil
}

func recoverLog(l log, peer string, maxMessages, maxBytes int) (*Outbox, error) {
	o := &Outbox{log: l, peer: peer, maxMessages: maxMessages, maxBytes: maxBytes, next: 1, pending: make(map[uint64][]byte)}
	first := true
	snapshot := false
	var previous uint64
	err := l.Replay(func(b []byte) error {
		var r record
		if !utf8.Valid(b) || json.Unmarshal(b, &r) != nil {
			return ErrInvalid
		}
		if first {
			first = false
			id, e := hex.DecodeString(r.Incarnation)
			if r.Type != "peer" || (r.Version != 1 && r.Version != 2) || r.Peer != peer || len(id) != 16 || e != nil || hex.EncodeToString(id) != r.Incarnation || len(r.Payload) != 0 {
				return ErrInvalid
			}
			if r.Version == 1 && r.ID != 0 || r.Version == 2 && r.ID == 0 {
				return ErrInvalid
			}
			if r.Version == 2 {
				o.next, snapshot = r.ID, true
			}
			o.incarnation = r.Incarnation
			return nil
		}
		if r.Version != 0 || r.Incarnation != "" {
			return ErrInvalid
		}
		switch r.Type {
		case "pending":
			if !snapshot || r.Peer != "" || r.ID <= previous || r.ID >= o.next || len(r.Payload) > math.MaxInt-o.bytes {
				return ErrInvalid
			}
			previous = r.ID
			o.pending[r.ID] = r.Payload
			o.order = append(o.order, r.ID)
			o.bytes += len(r.Payload)
		case "enqueue":
			snapshot = false
			if r.Peer != "" || r.ID != o.next || o.next == 0 || o.next == math.MaxUint64 || len(r.Payload) > math.MaxInt-o.bytes {
				return ErrInvalid
			}
			o.pending[r.ID] = r.Payload
			o.order = append(o.order, r.ID)
			o.bytes += len(r.Payload)
			o.next++
		case "ack":
			snapshot = false
			p, ok := o.pending[r.ID]
			if !ok || r.Peer != "" || len(r.Payload) != 0 {
				return ErrInvalid
			}
			o.bytes -= len(p)
			delete(o.pending, r.ID)
			// Bound stale order entries during replay, even for a long-lived WAL.
			if len(o.order)-len(o.pending) >= 1024 && len(o.order)-len(o.pending) >= len(o.pending) {
				o.compactOrder()
			}
		default:
			return ErrInvalid
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("outbox: replay: %w", err)
	}
	if first {
		var id [16]byte
		if _, err = rand.Read(id[:]); err != nil {
			return nil, fmt.Errorf("outbox: generate incarnation: %w", err)
		}
		o.incarnation = hex.EncodeToString(id[:])
		if err = o.append(record{Type: "peer", Peer: peer, Version: 1, Incarnation: o.incarnation}); err != nil {
			return nil, err
		}
	}
	if len(o.pending) > maxMessages || o.bytes > maxBytes {
		return nil, ErrFull
	}
	o.compactOrder()
	return o, nil
}

// Incarnation identifies this WAL generation. Combine it with a message ID
// when identifying messages across WAL recreation.
func (o *Outbox) Incarnation() string { return o.incarnation }
func (o *Outbox) append(r record) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(b) > storage.MaxRecordSize {
		return storage.ErrTooLarge
	}
	if err = o.log.Append(b); err != nil {
		o.failed = err
	}
	return err
}
func (o *Outbox) ready() error {
	if o.closed {
		return ErrClosed
	}
	if o.failed != nil {
		return o.failed
	}
	return nil
}

// Enqueue durably stores a copy and returns its peer-scoped ID. On a WAL
// error the outcome is uncertain; close and reopen before retrying.
func (o *Outbox) Enqueue(payload []byte) (uint64, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.ready(); err != nil {
		return 0, err
	}
	if len(o.pending) >= o.maxMessages || len(payload) > o.maxBytes-o.bytes {
		return 0, ErrFull
	}
	if o.next == 0 || o.next == math.MaxUint64 {
		return 0, ErrInvalid
	}
	id := o.next
	p := append([]byte(nil), payload...)
	if err := o.append(record{Type: "enqueue", ID: id, Payload: p}); err != nil {
		return 0, err
	}
	o.next++
	o.pending[id] = p
	o.order = append(o.order, id)
	o.bytes += len(p)
	return id, nil
}

// Ack durably removes a pending message. Unknown or already acknowledged IDs return ErrNotFound.
func (o *Outbox) Ack(id uint64) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.ready(); err != nil {
		return err
	}
	p, ok := o.pending[id]
	if !ok {
		return ErrNotFound
	}
	if err := o.append(record{Type: "ack", ID: id}); err != nil {
		return err
	}
	delete(o.pending, id)
	o.bytes -= len(p)
	o.compactOrder()
	return nil
}
func (o *Outbox) compactOrder() {
	n := 0
	for _, id := range o.order {
		if _, ok := o.pending[id]; ok {
			o.order[n] = id
			n++
		}
	}
	o.order = o.order[:n]
}

// Pending returns independent copies in enqueue order.
func (o *Outbox) Pending() ([]Message, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.ready(); err != nil {
		return nil, err
	}
	result := make([]Message, 0, len(o.pending))
	for _, id := range o.order {
		result = append(result, Message{ID: id, Payload: append([]byte(nil), o.pending[id]...)})
	}
	return result, nil
}
func (o *Outbox) Close() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return ErrClosed
	}
	o.closed = true
	return o.log.Close()
}
