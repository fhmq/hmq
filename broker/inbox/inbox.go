// Package inbox maintains a durable deduplication prefix for one sender.
// IDs start at 1 and must arrive without gaps. Event acceptance records ID and
// payload together; completion is not atomic with external business effects
// and does not provide exactly-once delivery. The log grows without bound.
// An incomplete or corrupt log is rejected on open.
package inbox

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"unicode/utf8"

	"github.com/fhmq/hmq/v3/broker/storage"
)

var ErrInvalid = errors.New("inbox: invalid argument or log")
var ErrGap = errors.New("inbox: message ID has a gap")
var ErrClosed = errors.New("inbox: closed")
var ErrMismatch = errors.New("inbox: payload mismatch or unavailable")

type record struct {
	Sender      string `json:"sender,omitempty"`
	Version     int    `json:"version,omitempty"`
	Incarnation string `json:"incarnation,omitempty"`
	ID          uint64 `json:"id,omitempty"`
	Kind        string `json:"kind,omitempty"`
	Payload     []byte `json:"payload,omitempty"`
}

type Event struct {
	ID      uint64
	Payload []byte
}

type Inbox struct {
	mu      sync.Mutex
	log     *storage.Log
	last    uint64
	closed  bool
	failed  error
	queue   bool
	entries map[uint64][]byte
	pending map[uint64]bool
}

// Open opens a log dedicated to sender. Its parent directory must exist.
func Open(path, sender string) (*Inbox, error) {
	return open(path, sender, "")
}

// OpenQueue opens a durable event queue bound to an authenticated sender and
// its outbox incarnation. Use a separate path for each sender/incarnation pair;
// an existing log cannot be rebound. Incarnation is the lowercase hex encoding
// of the 16-byte outbox incarnation. Legacy ID-only logs cannot be upgraded.
// Callers must authenticate the sender and validate the origin of incarnation.
func OpenQueue(path, sender, incarnation string) (*Inbox, error) {
	id, err := hex.DecodeString(incarnation)
	if err != nil || len(id) != 16 || hex.EncodeToString(id) != incarnation {
		return nil, ErrInvalid
	}
	return open(path, sender, incarnation)
}

func open(path, sender, incarnation string) (*Inbox, error) {
	if sender == "" || !utf8.ValidString(sender) {
		return nil, ErrInvalid
	}
	l, err := storage.Open(path)
	if err != nil {
		return nil, err
	}
	i := &Inbox{log: l, queue: incarnation != "", entries: make(map[uint64][]byte), pending: make(map[uint64]bool)}
	first := true
	err = l.Replay(func(b []byte) error {
		var r record
		if !utf8.Valid(b) || json.Unmarshal(b, &r) != nil {
			return ErrInvalid
		}
		if first {
			first = false
			version := 0
			if i.queue {
				version = 1
			}
			if r.Sender != sender || r.Version != version || r.Incarnation != incarnation || r.ID != 0 || r.Kind != "" || r.Payload != nil {
				return ErrInvalid
			}
			return nil
		}
		if r.Version != 0 || r.Incarnation != "" {
			return ErrInvalid
		}
		if r.Kind == "complete" {
			if r.Sender != "" || r.Payload != nil || !i.pending[r.ID] {
				return ErrInvalid
			}
			delete(i.pending, r.ID)
			return nil
		}
		if r.Sender != "" || (r.Kind != "" && r.Kind != "event") || (i.queue && r.Kind != "event") || (r.Kind == "" && r.Payload != nil) || i.last == math.MaxUint64 || r.ID != i.last+1 {
			return ErrInvalid
		}
		i.last = r.ID
		if r.Kind == "event" {
			i.entries[r.ID] = append([]byte{}, r.Payload...)
			i.pending[r.ID] = true
		}
		return nil
	})
	if err == nil && first {
		header := record{Sender: sender, Incarnation: incarnation}
		if i.queue {
			header.Version = 1
		}
		err = i.append(header)
	}
	if err != nil {
		l.Close()
		return nil, fmt.Errorf("inbox: open: %w", err)
	}
	return i, nil
}

func (i *Inbox) append(r record) error {
	b, err := json.Marshal(r)
	if err == nil {
		err = i.log.Append(b)
	}
	if err != nil && !errors.Is(err, storage.ErrTooLarge) {
		i.failed = err
	}
	return err
}

// Accept atomically persists the next ID and returns true. Previously accepted
// IDs return false without writing; zero and gaps are rejected. This compares
// IDs only, not payloads. After a write error the result is uncertain: close
// and reopen before retrying. A corrupt or incomplete WAL prevents reopening.
func (i *Inbox) Accept(id uint64) (bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return false, ErrClosed
	}
	if i.failed != nil {
		return false, i.failed
	}
	if i.queue {
		return false, ErrInvalid
	}
	if id == 0 {
		return false, ErrInvalid
	}
	if id <= i.last {
		return false, nil
	}
	if i.last == math.MaxUint64 || id != i.last+1 {
		return false, ErrGap
	}
	if err := i.append(record{ID: id}); err != nil {
		return false, err
	}
	i.last = id
	return true, nil
}

// AcceptEvent durably records an ID and its payload together. Duplicate IDs
// must have identical payloads, even after completion. Legacy ID-only records
// cannot be verified and return ErrMismatch. Write errors require reopening.
func (i *Inbox) AcceptEvent(id uint64, payload []byte) (bool, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return false, ErrClosed
	}
	if i.failed != nil {
		return false, i.failed
	}
	if id == 0 {
		return false, ErrInvalid
	}
	if id <= i.last {
		p, ok := i.entries[id]
		if !ok || !bytes.Equal(p, payload) {
			return false, ErrMismatch
		}
		return false, nil
	}
	if i.last == math.MaxUint64 || id != i.last+1 {
		return false, ErrGap
	}
	p := append([]byte{}, payload...)
	if err := i.append(record{Kind: "event", ID: id, Payload: p}); err != nil {
		return false, err
	}
	i.last = id
	i.entries[id] = p
	i.pending[id] = true
	return true, nil
}

// Pending returns independent payload copies in ID order. It does not claim
// events: concurrent consumers may observe the same event.
func (i *Inbox) Pending() ([]Event, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return nil, ErrClosed
	}
	if i.failed != nil {
		return nil, i.failed
	}
	result := make([]Event, 0, len(i.pending))
	for id := uint64(1); id != 0 && id <= i.last; id++ {
		if i.pending[id] {
			result = append(result, Event{ID: id, Payload: append([]byte{}, i.entries[id]...)})
		}
	}
	return result, nil
}

// Complete durably marks an event finished. Repeated completion is harmless.
func (i *Inbox) Complete(id uint64) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return ErrClosed
	}
	if i.failed != nil {
		return i.failed
	}
	if _, ok := i.entries[id]; !ok {
		return ErrInvalid
	}
	if !i.pending[id] {
		return nil
	}
	if err := i.append(record{Kind: "complete", ID: id}); err != nil {
		return err
	}
	delete(i.pending, id)
	return nil
}

// Close releases the log lock.
func (i *Inbox) Close() error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return ErrClosed
	}
	i.closed = true
	return i.log.Close()
}
