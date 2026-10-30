// Package eventstore implements a durable single-node event and delivery log.
package eventstore

import (
	"bytes"
	"container/list"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"unicode/utf8"

	"github.com/fhmq/hmq/v3/broker/storage"
)

var ErrConflict = errors.New("eventstore: conflicting duplicate")
var ErrCapacity = errors.New("eventstore: capacity exceeded")
var ErrNotFound = errors.New("eventstore: event or target not found")
var ErrInvalid = errors.New("eventstore: invalid event")
var ErrUnavailable = errors.New("eventstore: reopen after append failure or close")

// Limits bounds retained events and their payload plus identifier bytes. Zero
// means unlimited. Entries, including completed ones, remain for deduplication.
type Limits struct {
	MaxEvents int
	MaxBytes  int64
}
type Event struct {
	ID        string
	Payload   []byte
	Targets   []string
	Confirmed []string
}
type record struct {
	Type    string   `json:"type"`
	ID      string   `json:"id"`
	Payload []byte   `json:"payload,omitempty"`
	Targets []string `json:"targets,omitempty"`
	Target  string   `json:"target,omitempty"`
}
type entry struct {
	sequence  uint64
	payload   []byte
	targets   []string
	confirmed map[string]bool
	pending   map[string]*list.Element
}
type log interface {
	Append([]byte) error
	Replay(func([]byte) error) error
	Close() error
}
type Store struct {
	mu          sync.Mutex
	log         log
	limits      Limits
	entries     map[string]*entry
	pending     map[string]*pendingQueue
	bytes       int64
	unavailable bool
}

// BindIdentity reserves a record in a dedicated log. Existing unbound logs are
// rejected, since their ownership cannot be established safely.
func (s *Store) BindIdentity(identity []byte) error {
	const id = "!hmq-log-identity-v1"
	const target = "!hmq-log-identity"
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unavailable {
		return ErrUnavailable
	}
	if e, ok := s.entries[id]; ok {
		if !bytes.Equal(e.payload, identity) || len(e.targets) != 1 || e.targets[0] != target {
			return ErrConflict
		}
		return nil
	}
	if len(s.entries) != 0 || len(identity) == 0 {
		return ErrConflict
	}
	r, err := normalized(id, identity, []string{target})
	if err != nil {
		return err
	}
	if (s.limits.MaxEvents > 0 && len(s.entries) >= s.limits.MaxEvents) || (s.limits.MaxBytes > 0 && size(r) > s.limits.MaxBytes-s.bytes) {
		return ErrCapacity
	}
	return s.append(r)
}

// Open recovers the entire log and rejects invalid histories or limits smaller
// than the recovered state. The containing directory must already exist.
func Open(path string, limits Limits) (*Store, error) {
	if limits.MaxEvents < 0 || limits.MaxBytes < 0 {
		return nil, ErrInvalid
	}
	l, err := storage.Open(path)
	if err != nil {
		return nil, err
	}
	s, err := recoverStore(l, limits)
	if err != nil {
		l.Close()
		return nil, err
	}
	return s, nil
}
func recoverStore(l log, limits Limits) (*Store, error) {
	s := &Store{log: l, limits: limits, entries: make(map[string]*entry), pending: make(map[string]*pendingQueue)}
	err := l.Replay(func(b []byte) error {
		if !utf8.Valid(b) {
			return ErrInvalid
		}
		var r record
		if err := json.Unmarshal(b, &r); err != nil {
			return err
		}
		return s.apply(r)
	})
	if err != nil {
		return nil, fmt.Errorf("eventstore: invalid history: %w", err)
	}
	return s, nil
}
func normalized(id string, payload []byte, targets []string) (record, error) {
	if id == "" || !utf8.ValidString(id) || len(targets) == 0 {
		return record{}, ErrInvalid
	}
	t := append([]string(nil), targets...)
	sort.Strings(t)
	for i, v := range t {
		if v == "" || !utf8.ValidString(v) || (i > 0 && t[i-1] == v) {
			return record{}, ErrInvalid
		}
	}
	return record{Type: "event", ID: id, Payload: append([]byte(nil), payload...), Targets: t}, nil
}
func size(r record) int64 {
	n := int64(len(r.ID) + len(r.Payload))
	for _, t := range r.Targets {
		n += int64(len(t))
	}
	return n
}
func (s *Store) apply(r record) error {
	switch r.Type {
	case "event":
		n, err := normalized(r.ID, r.Payload, r.Targets)
		if err != nil || r.Target != "" {
			return ErrInvalid
		}
		if _, ok := s.entries[r.ID]; ok {
			return ErrConflict
		}
		if (s.limits.MaxEvents > 0 && len(s.entries) >= s.limits.MaxEvents) || (s.limits.MaxBytes > 0 && size(n) > s.limits.MaxBytes-s.bytes) {
			return ErrCapacity
		}
		e := &entry{sequence: uint64(len(s.entries)) + 1, payload: n.Payload, targets: n.Targets, confirmed: make(map[string]bool), pending: make(map[string]*list.Element)}
		s.entries[r.ID] = e
		for _, target := range n.Targets {
			q := s.pending[target]
			if q == nil {
				q = &pendingQueue{}
				s.pending[target] = q
			}
			e.pending[target] = q.items.PushBack(r.ID)
			q.root = insertPending(q.root, e.sequence, e.pending[target])
		}
		s.bytes += size(n)
	case "ack":
		e := s.entries[r.ID]
		if e == nil || r.Target == "" || len(r.Payload) != 0 || len(r.Targets) != 0 {
			return ErrInvalid
		}
		i := sort.SearchStrings(e.targets, r.Target)
		if i == len(e.targets) || e.targets[i] != r.Target || e.confirmed[r.Target] {
			return ErrInvalid
		}
		e.confirmed[r.Target] = true
		q := s.pending[r.Target]
		q.root = removePending(q.root, e.sequence)
		q.items.Remove(e.pending[r.Target])
		delete(e.pending, r.Target)
		if q.items.Len() == 0 {
			delete(s.pending, r.Target)
		}
	default:
		return ErrInvalid
	}
	return nil
}
func (s *Store) append(r record) error {
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(b) > storage.MaxRecordSize {
		return storage.ErrTooLarge
	}
	if err = s.log.Append(b); err != nil {
		s.unavailable = true
		return err
	}
	return s.apply(r)
}

// Put atomically persists an event and its complete target set. Target order is
// immaterial; a repeated ID must have identical payload and targets.
func (s *Store) Put(id string, payload []byte, targets []string) error {
	r, err := normalized(id, payload, targets)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unavailable {
		return ErrUnavailable
	}
	if e := s.entries[id]; e != nil {
		if !bytes.Equal(e.payload, r.Payload) || len(e.targets) != len(r.Targets) {
			return ErrConflict
		}
		for i := range e.targets {
			if e.targets[i] != r.Targets[i] {
				return ErrConflict
			}
		}
		return nil
	}
	if (s.limits.MaxEvents > 0 && len(s.entries) >= s.limits.MaxEvents) || (s.limits.MaxBytes > 0 && size(r) > s.limits.MaxBytes-s.bytes) {
		return ErrCapacity
	}
	return s.append(r)
}

// Ack durably confirms one target; repeated confirmations are idempotent.
func (s *Store) Ack(id, target string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unavailable {
		return ErrUnavailable
	}
	e := s.entries[id]
	if e == nil {
		return ErrNotFound
	}
	i := sort.SearchStrings(e.targets, target)
	if i == len(e.targets) || e.targets[i] != target {
		return ErrNotFound
	}
	if e.confirmed[target] {
		return nil
	}
	return s.append(record{Type: "ack", ID: id, Target: target})
}

// Get returns independent copies of the event and confirmed targets.
func (s *Store) Get(id string) (Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unavailable {
		return Event{}, ErrUnavailable
	}
	e := s.entries[id]
	if e == nil {
		return Event{}, ErrNotFound
	}
	return copyEvent(id, e), nil
}

func copyEvent(id string, e *entry) Event {
	out := Event{ID: id, Payload: append([]byte(nil), e.payload...), Targets: append([]string(nil), e.targets...)}
	for _, t := range e.targets {
		if e.confirmed[t] {
			out.Confirmed = append(out.Confirmed, t)
		}
	}
	return out
}

// Pending returns at most limit events awaiting target, in first successful Put
// order. The result is a snapshot with independent copies. limit must be
// positive and target must be a nonempty valid UTF-8 identifier.
func (s *Store) Pending(target string, limit int) ([]Event, error) {
	events, _, err := s.PendingAfter(target, 0, limit)
	return events, err
}

// PendingAfter returns pending events in insertion order after cursor. The
// returned cursor remains valid even if its event is subsequently ACKed.
// A zero cursor starts at the beginning; an empty result marks the end. Locating
// the first event takes O(log n), followed by O(limit) sequential traversal.
func (s *Store) PendingAfter(target string, cursor uint64, limit int) ([]Event, uint64, error) {
	return s.PendingUntil(target, cursor, ^uint64(0), limit)
}

// Sequence returns the latest durable insertion sequence. It can be used as
// an inclusive upper bound for a finite scan despite concurrent insertions.
func (s *Store) Sequence() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unavailable {
		return 0, ErrUnavailable
	}
	return uint64(len(s.entries)), nil
}

// PendingUntil is PendingAfter restricted to sequences at most through.
func (s *Store) PendingUntil(target string, cursor, through uint64, limit int) ([]Event, uint64, error) {
	if target == "" || !utf8.ValidString(target) || limit <= 0 {
		return nil, cursor, ErrInvalid
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unavailable {
		return nil, cursor, ErrUnavailable
	}
	var out []Event
	q := s.pending[target]
	if q == nil {
		return out, cursor, nil
	}
	for node := pendingAfter(q.root, cursor); node != nil; node = node.Next() {
		id := node.Value.(string)
		e := s.entries[id]
		if e.sequence > through {
			break
		}
		out = append(out, copyEvent(id, e))
		cursor = e.sequence
		if len(out) == limit {
			break
		}
	}
	return out, cursor, nil
}
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log == nil {
		return ErrUnavailable
	}
	s.unavailable = true
	err := s.log.Close()
	s.log = nil
	return err
}
