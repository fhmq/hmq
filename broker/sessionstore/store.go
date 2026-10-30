// Package sessionstore provides an independent, single-node session snapshot store.
package sessionstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"sync"
	"unicode/utf8"

	"github.com/fhmq/hmq/v3/broker/storage"
)

var ErrConflict = errors.New("sessionstore: version conflict")
var ErrInvalid = errors.New("sessionstore: invalid state")
var ErrCorrupt = errors.New("sessionstore: invalid log record")

// Message is an unsent offline publication. It intentionally has no packet ID.
type Message struct {
	Retain  bool   `json:"retain,omitempty"`
	Topic   string `json:"topic"`
	Payload []byte `json:"payload"`
	QoS     byte   `json:"qos"`
}

// State is the complete state managed by this package, not a complete MQTT session.
type State struct {
	Epoch         uint64              `json:"epoch,omitempty"`
	Subscriptions map[string]byte     `json:"subscriptions"`
	Queue         []Message           `json:"queue"`
	Inflight      map[uint16]Inflight `json:"inflight,omitempty"`
	LastSequence  uint64              `json:"last_sequence,omitempty"`
}

// Phase records the outbound MQTT handshake state. A recovered Publish must be
// retransmitted with DUP; a recovered PubRel must be retransmitted as PUBREL.
type Phase string

const (
	Publish Phase = "publish"
	PubRel  Phase = "pubrel"
)

type Inflight struct {
	Message  Message `json:"message"`
	Phase    Phase   `json:"phase"`
	Sequence uint64  `json:"sequence,omitempty"`
}

type Ack struct {
	PacketID uint16 `json:"packet_id"`
	Type     string `json:"type"` // PUBACK, PUBREC, or PUBCOMP
}

type record struct {
	Format   int    `json:"format"`
	Sequence uint64 `json:"sequence"`
	ClientID string `json:"client_id"`
	State    *State `json:"state"`
	Batch    *Batch `json:"batch,omitempty"`
}

// Batch changes one existing session atomically. Remove first dequeues the
// existing queue prefix, then Enqueue appends messages in order.
type Batch struct {
	ExpectedEpoch       *uint64            `json:"expected_epoch,omitempty"`
	NextEpoch           *uint64            `json:"next_epoch,omitempty"`
	Start               map[uint16]Message `json:"start,omitempty"`
	SetSubscriptions    map[string]byte    `json:"set_subscriptions,omitempty"`
	RemoveSubscriptions []string           `json:"remove_subscriptions,omitempty"`
	Remove              int                `json:"remove,omitempty"`
	Enqueue             []Message          `json:"enqueue,omitempty"`
	// Move assigns packet IDs to the original queue prefix, in order. It
	// cannot be combined with Remove, which discards a queue prefix.
	Move []uint16 `json:"move,omitempty"`
	Acks []Ack    `json:"acks,omitempty"`
}
type entry struct {
	version uint64
	state   *State
}

type Store struct {
	mu       sync.Mutex
	log      *storage.Log
	sequence uint64
	entries  map[string]entry // Tombstones retain versions to prevent ABA.
	closed   bool
	failed   error
}

// validSurrogates checks JSON string escapes before encoding/json can replace
// unpaired UTF-16 surrogate escapes with U+FFFD.
func validSurrogates(data []byte) bool {
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
		if i >= len(data) {
			return false
		}
		if data[i] != 'u' {
			continue
		}
		if i+4 >= len(data) {
			return false
		}
		value, err := strconv.ParseUint(string(data[i+1:i+5]), 16, 16)
		if err != nil {
			return false
		}
		i += 4
		if value >= 0xdc00 && value <= 0xdfff {
			return false
		}
		if value >= 0xd800 && value <= 0xdbff {
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

func validate(id string, s *State) error {
	if id == "" || !utf8.ValidString(id) {
		return ErrInvalid
	}
	if s == nil {
		return nil
	}
	for filter, qos := range s.Subscriptions {
		if filter == "" || !utf8.ValidString(filter) || qos > 2 {
			return ErrInvalid
		}
	}
	for _, m := range s.Queue {
		if m.Topic == "" || !utf8.ValidString(m.Topic) || m.QoS < 1 || m.QoS > 2 {
			return ErrInvalid
		}
	}
	seen := make(map[uint64]bool, len(s.Inflight))
	for id, item := range s.Inflight {
		if id == 0 || validate("x", &State{Queue: []Message{item.Message}}) != nil ||
			(item.Phase != Publish && item.Phase != PubRel) || (item.Phase == PubRel && item.Message.QoS != 2) ||
			item.Sequence == 0 || item.Sequence > s.LastSequence || seen[item.Sequence] {
			return ErrInvalid
		}
		seen[item.Sequence] = true
	}
	return nil
}

func applyBatch(id string, state *State, b *Batch, legacy bool) (*State, error) {
	if state != nil && b != nil && b.ExpectedEpoch != nil && *b.ExpectedEpoch != state.Epoch {
		return nil, ErrConflict
	}
	if state == nil || b == nil || b.Remove < 0 || b.Remove > len(state.Queue) ||
		(len(b.Move) > 0 && b.Remove != 0) || len(b.Move) > len(state.Queue) {
		return nil, ErrInvalid
	}
	if err := validate(id, &State{Subscriptions: b.SetSubscriptions, Queue: b.Enqueue}); err != nil {
		return nil, err
	}
	for _, filter := range b.RemoveSubscriptions {
		if filter == "" || !utf8.ValidString(filter) {
			return nil, ErrInvalid
		}
	}
	next := &State{LastSequence: state.LastSequence, Epoch: state.Epoch}
	if b.NextEpoch != nil {
		if *b.NextEpoch <= state.Epoch {
			return nil, ErrInvalid
		}
		next.Epoch = *b.NextEpoch
	}
	if state.Inflight != nil {
		next.Inflight = make(map[uint16]Inflight, len(state.Inflight))
		for id, item := range state.Inflight {
			next.Inflight[id] = item
		}
	}
	acked := make(map[uint16]bool, len(b.Acks))
	for _, ack := range b.Acks {
		if !legacy && acked[ack.PacketID] {
			return nil, ErrInvalid
		}
		acked[ack.PacketID] = true
		item, ok := next.Inflight[ack.PacketID]
		if !ok {
			return nil, ErrInvalid
		}
		switch {
		case ack.Type == "PUBACK" && item.Message.QoS == 1 && item.Phase == Publish:
			delete(next.Inflight, ack.PacketID)
		case ack.Type == "PUBREC" && item.Message.QoS == 2 && item.Phase == Publish:
			item.Phase = PubRel
			next.Inflight[ack.PacketID] = item
		case ack.Type == "PUBCOMP" && item.Message.QoS == 2 && item.Phase == PubRel:
			delete(next.Inflight, ack.PacketID)
		default:
			return nil, ErrInvalid
		}
	}
	if len(b.Move) > 0 && next.Inflight == nil {
		next.Inflight = make(map[uint16]Inflight)
	}
	for i, id := range b.Move {
		if id == 0 || (!legacy && acked[id]) || next.LastSequence == ^uint64(0) {
			return nil, ErrInvalid
		}
		if _, exists := next.Inflight[id]; exists {
			return nil, ErrInvalid
		}
		next.LastSequence++
		next.Inflight[id] = Inflight{Message: state.Queue[i], Phase: Publish, Sequence: next.LastSequence}
	}
	startIDs := make([]int, 0, len(b.Start))
	for packetID := range b.Start {
		startIDs = append(startIDs, int(packetID))
	}
	sort.Ints(startIDs)
	for _, value := range startIDs {
		packetID := uint16(value)
		message := b.Start[packetID]
		if packetID == 0 || acked[packetID] || next.LastSequence == ^uint64(0) {
			return nil, ErrInvalid
		}
		if _, exists := next.Inflight[packetID]; exists {
			return nil, ErrInvalid
		}
		if validate(id, &State{Queue: []Message{message}}) != nil {
			return nil, ErrInvalid
		}
		if next.Inflight == nil {
			next.Inflight = make(map[uint16]Inflight)
		}
		next.LastSequence++
		next.Inflight[packetID] = Inflight{Message: message, Phase: Publish, Sequence: next.LastSequence}
	}
	if state.Subscriptions != nil {
		next.Subscriptions = make(map[string]byte, len(state.Subscriptions))
		for filter, qos := range state.Subscriptions {
			next.Subscriptions[filter] = qos
		}
	}
	if len(b.SetSubscriptions) > 0 && next.Subscriptions == nil {
		next.Subscriptions = make(map[string]byte)
	}
	for _, filter := range b.RemoveSubscriptions {
		delete(next.Subscriptions, filter)
	}
	for filter, qos := range b.SetSubscriptions {
		next.Subscriptions[filter] = qos
	}
	next.Queue = append(next.Queue, state.Queue[b.Remove+len(b.Move):]...)
	next.Queue = append(next.Queue, clone(&State{Queue: b.Enqueue}).Queue...)
	return next, nil
}

func clone(s *State) *State {
	if s == nil {
		return nil
	}
	c := &State{LastSequence: s.LastSequence, Epoch: s.Epoch}
	if s.Subscriptions != nil {
		c.Subscriptions = make(map[string]byte, len(s.Subscriptions))
		for k, v := range s.Subscriptions {
			c.Subscriptions[k] = v
		}
	}
	if s.Queue != nil {
		c.Queue = make([]Message, len(s.Queue))
		for i, m := range s.Queue {
			c.Queue[i] = m
			c.Queue[i].Payload = bytes.Clone(m.Payload)
		}
	}
	if s.Inflight != nil {
		c.Inflight = make(map[uint16]Inflight, len(s.Inflight))
		for id, item := range s.Inflight {
			item.Message.Payload = bytes.Clone(item.Message.Payload)
			c.Inflight[id] = item
		}
	}
	return c
}

// ReadQueue returns at most limit messages starting at offset, with at most
// maxBytes of payload. An oversized next message yields ErrTooLarge so callers
// can increase the bound. The version identifies the snapshot for CAS and
// subsequent pages; restart pagination if the version changes.
func (s *Store) ReadQueue(id string, offset, limit, maxBytes int) ([]Message, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, 0, storage.ErrClosed
	}
	if s.failed != nil {
		return nil, 0, s.failed
	}
	if id == "" || !utf8.ValidString(id) || offset < 0 || limit <= 0 || maxBytes <= 0 {
		return nil, 0, ErrInvalid
	}
	e := s.entries[id]
	if e.state == nil {
		return nil, e.version, nil
	}
	if offset > len(e.state.Queue) {
		return nil, e.version, ErrInvalid
	}
	var result []Message
	used := 0
	for _, m := range e.state.Queue[offset:] {
		if len(result) == limit {
			break
		}
		if len(m.Payload) > maxBytes-used {
			if len(result) == 0 {
				return nil, e.version, storage.ErrTooLarge
			}
			break
		}
		m.Payload = bytes.Clone(m.Payload)
		result = append(result, m)
		used += len(m.Payload)
	}
	return result, e.version, nil
}

type InflightEntry struct {
	PacketID uint16
	Inflight Inflight
}

// ReadInflight returns entries in send order strictly after afterSequence.
// As with ReadQueue, callers must compare versions across pages.
func (s *Store) ReadInflight(id string, afterSequence uint64, limit, maxBytes int) ([]InflightEntry, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, 0, storage.ErrClosed
	}
	if s.failed != nil {
		return nil, 0, s.failed
	}
	if id == "" || !utf8.ValidString(id) || limit <= 0 || maxBytes <= 0 {
		return nil, 0, ErrInvalid
	}
	e := s.entries[id]
	if e.state == nil {
		return nil, e.version, nil
	}
	items := make([]InflightEntry, 0, len(e.state.Inflight))
	for packetID, item := range e.state.Inflight {
		if item.Sequence > afterSequence {
			items = append(items, InflightEntry{PacketID: packetID, Inflight: item})
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Inflight.Sequence < items[j].Inflight.Sequence })
	var result []InflightEntry
	used := 0
	for _, entry := range items {
		if len(result) == limit {
			break
		}
		item := entry.Inflight
		if len(item.Message.Payload) > maxBytes-used {
			if len(result) == 0 {
				return nil, e.version, storage.ErrTooLarge
			}
			break
		}
		item.Message.Payload = bytes.Clone(item.Message.Payload)
		used += len(item.Message.Payload)
		result = append(result, InflightEntry{PacketID: entry.PacketID, Inflight: item})
	}
	return result, e.version, nil
}

// insertPage keeps only the lexicographically first limit candidates.
func insertPage(page []string, value string, limit int) []string {
	i := sort.SearchStrings(page, value)
	if i >= limit {
		return page
	}
	page = append(page, "")
	copy(page[i+1:], page[i:])
	page[i] = value
	if len(page) > limit {
		page = page[:limit]
	}
	return page
}

// List returns live client IDs strictly after afterID. The returned global
// sequence must match across pages; any successful write invalidates a scan.
// maxBytes bounds the sum of returned UTF-8 ID bytes.
func (s *Store) List(afterID string, limit, maxBytes int) ([]string, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, 0, storage.ErrClosed
	}
	if s.failed != nil {
		return nil, 0, s.failed
	}
	if !utf8.ValidString(afterID) || limit <= 0 || maxBytes <= 0 {
		return nil, 0, ErrInvalid
	}
	var page []string
	for id, e := range s.entries {
		if e.state != nil && id > afterID {
			page = insertPage(page, id, limit)
		}
	}
	used := 0
	for i, id := range page {
		if len(id) > maxBytes-used {
			if i == 0 {
				return nil, s.sequence, storage.ErrTooLarge
			}
			return page[:i], s.sequence, nil
		}
		used += len(id)
	}
	return page, s.sequence, nil
}

type Subscription struct {
	Filter string
	QoS    byte
}

// ReadSubscriptions returns filters strictly after afterFilter in lexical
// order, bounded by count and total filter bytes. Compare versions across pages.
func (s *Store) ReadSubscriptions(id, afterFilter string, limit, maxBytes int) ([]Subscription, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, 0, storage.ErrClosed
	}
	if s.failed != nil {
		return nil, 0, s.failed
	}
	if id == "" || !utf8.ValidString(id) || !utf8.ValidString(afterFilter) || limit <= 0 || maxBytes <= 0 {
		return nil, 0, ErrInvalid
	}
	e := s.entries[id]
	if e.state == nil {
		return nil, e.version, nil
	}
	var filters []string
	for filter := range e.state.Subscriptions {
		if filter > afterFilter {
			filters = insertPage(filters, filter, limit)
		}
	}
	var result []Subscription
	used := 0
	for _, filter := range filters {
		if len(filter) > maxBytes-used {
			if len(result) == 0 {
				return nil, e.version, storage.ErrTooLarge
			}
			break
		}
		result = append(result, Subscription{Filter: filter, QoS: e.state.Subscriptions[filter]})
		used += len(filter)
	}
	return result, e.version, nil
}

// Open replays the entire log and fails closed on malformed records.
func Open(path string) (*Store, error) {
	l, err := storage.Open(path)
	if err != nil {
		return nil, err
	}
	s := &Store{log: l, entries: make(map[string]entry)}
	err = l.Replay(func(data []byte) error {
		if !utf8.Valid(data) || !validSurrogates(data) {
			return ErrCorrupt
		}
		var r record
		d := json.NewDecoder(bytes.NewReader(data))
		d.DisallowUnknownFields()
		if err := d.Decode(&r); err != nil {
			return fmt.Errorf("%w: %v", ErrCorrupt, err)
		}
		if err := d.Decode(new(any)); err != io.EOF {
			return ErrCorrupt
		}
		if r.Sequence == 0 || r.Sequence != s.sequence+1 || r.ClientID == "" || !utf8.ValidString(r.ClientID) {
			return ErrCorrupt
		}
		if r.Format == 1 || r.Format == 3 {
			if r.Batch != nil {
				return ErrCorrupt
			}
			if r.Format == 1 && r.State != nil {
				// Legacy snapshots did not record an order. More than one
				// inflight message cannot be recovered without guessing.
				if r.State.LastSequence != 0 || len(r.State.Inflight) > 1 {
					return ErrCorrupt
				}
				for id, item := range r.State.Inflight {
					if item.Sequence != 0 {
						return ErrCorrupt
					}
					item.Sequence = 1
					r.State.LastSequence = 1
					r.State.Inflight[id] = item
				}
			}
			if validate(r.ClientID, r.State) != nil {
				return ErrCorrupt
			}
		} else if r.Format == 2 || r.Format == 4 {
			if r.State != nil {
				return ErrCorrupt
			}
			var err error
			r.State, err = applyBatch(r.ClientID, s.entries[r.ClientID].state, r.Batch, r.Format == 2)
			if err != nil {
				return ErrCorrupt
			}
		} else {
			return ErrCorrupt
		}
		s.sequence = r.Sequence
		s.entries[r.ClientID] = entry{r.Sequence, r.State}
		return nil
	})
	if err != nil {
		_ = l.Close()
		return nil, err
	}
	return s, nil
}

// Get returns an independent snapshot and its version. A missing or deleted
// session has nil state; its version is retained and must be used for Put.
func (s *Store) Get(id string) (*State, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, 0, storage.ErrClosed
	}
	if s.failed != nil {
		return nil, 0, s.failed
	}
	if id == "" || !utf8.ValidString(id) {
		return nil, 0, ErrInvalid
	}
	e := s.entries[id]
	return clone(e.state), e.version, nil
}

// Inspect returns CAS metadata and one inflight entry without copying the backlog.
func (s *Store) Inspect(id string, packetID uint16) (uint64, bool, Inflight, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, false, Inflight{}, false, storage.ErrClosed
	}
	if s.failed != nil {
		return 0, false, Inflight{}, false, s.failed
	}
	e := s.entries[id]
	if e.state == nil {
		return e.version, false, Inflight{}, false, nil
	}
	item, ok := e.state.Inflight[packetID]
	item.Message.Payload = bytes.Clone(item.Message.Payload)
	return e.version, true, item, ok, nil
}

// Put atomically replaces one session if expected matches its current version.
// A nil state deletes it. On an append error the outcome is uncertain; close
// and reopen before retrying. Callers must not mutate state concurrently with Put.
func (s *Store) Put(id string, expected uint64, state *State) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, storage.ErrClosed
	}
	if s.failed != nil {
		return 0, s.failed
	}
	if err := validate(id, state); err != nil {
		return 0, err
	}
	if s.entries[id].version != expected {
		return 0, ErrConflict
	}
	if state != nil && s.entries[id].state != nil && state.LastSequence < s.entries[id].state.LastSequence {
		return 0, ErrInvalid
	}
	if s.sequence == ^uint64(0) {
		return 0, ErrInvalid
	}
	r := record{Format: 3, Sequence: s.sequence + 1, ClientID: id, State: clone(state)}
	data, err := json.Marshal(r)
	if err != nil {
		return 0, err
	}
	if len(data) > storage.MaxRecordSize {
		return 0, storage.ErrTooLarge
	}
	if err = s.log.Append(data); err != nil {
		s.failed = err
		return 0, err
	}
	s.sequence = r.Sequence
	s.entries[id] = entry{r.Sequence, r.State}
	return r.Sequence, nil
}

// Apply commits a batch to an existing session using the same CAS version as Put.
// The total backlog can exceed MaxRecordSize; each individual batch cannot.
// Input must not be mutated concurrently. Failed appends require reopening.
func (s *Store) Apply(id string, expected uint64, batch Batch) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, storage.ErrClosed
	}
	if s.failed != nil {
		return 0, s.failed
	}
	if id == "" || !utf8.ValidString(id) {
		return 0, ErrInvalid
	}
	if s.entries[id].version != expected {
		return 0, ErrConflict
	}
	if s.sequence == ^uint64(0) {
		return 0, ErrInvalid
	}
	next, err := applyBatch(id, s.entries[id].state, &batch, false)
	if err != nil {
		return 0, err
	}
	r := record{Format: 4, Sequence: s.sequence + 1, ClientID: id, Batch: &batch}
	data, err := json.Marshal(r)
	if err != nil {
		return 0, err
	}
	if len(data) > storage.MaxRecordSize {
		return 0, storage.ErrTooLarge
	}
	if err = s.log.Append(data); err != nil {
		s.failed = err
		return 0, err
	}
	s.sequence = r.Sequence
	s.entries[id] = entry{r.Sequence, next}
	return r.Sequence, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return storage.ErrClosed
	}
	s.closed = true
	return s.log.Close()
}
