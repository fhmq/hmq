package broker

import (
	stdjson "encoding/json"
	"errors"
	"unicode/utf8"

	"github.com/fhmq/hmq/v4/broker/eventstore"
	"github.com/fhmq/hmq/v4/broker/reliable"
)

// fanoutStore owns the only write path for new events. Link views may only
// read and acknowledge them, preventing partially persisted fanouts.
type fanoutStore struct {
	store        reliable.Store
	peers        []string
	maxFrameSize uint32
}

func newFanoutStore(store reliable.Store, peers []string, maxFrameSize uint32) (*fanoutStore, error) {
	if store == nil || len(peers) == 0 || maxFrameSize == ^uint32(0) {
		return nil, eventstore.ErrInvalid
	}
	seen := make(map[string]bool, len(peers))
	for _, peer := range peers {
		if peer == "" || !utf8.ValidString(peer) || seen[peer] {
			return nil, eventstore.ErrInvalid
		}
		seen[peer] = true
	}
	if maxFrameSize == 0 {
		maxFrameSize = reliable.DefaultMaxFrameSize
	}
	return &fanoutStore{store: store, peers: append([]string(nil), peers...), maxFrameSize: maxFrameSize}, nil
}

func (f *fanoutStore) enqueue(id string, payload []byte) error {
	if id == "" || !utf8.ValidString(id) {
		return reliable.ErrFrame
	}
	// Keep this wire representation identical to reliable's data frame.
	b, err := stdjson.Marshal(struct {
		Version int    `json:"version"`
		Type    string `json:"type"`
		ID      string `json:"id"`
		Payload []byte `json:"payload,omitempty"`
	}{1, "data", id, payload})
	if err != nil {
		return err
	}
	if uint64(len(b)) > uint64(f.maxFrameSize) {
		return reliable.ErrFrameTooLarge
	}
	return f.store.Put(id, payload, append([]string(nil), f.peers...))
}

func (f *fanoutStore) view(peer string) (reliable.Store, error) {
	for _, p := range f.peers {
		if p == peer {
			return &fanoutView{store: f.store, peer: peer}, nil
		}
	}
	return nil, reliable.ErrPeer
}

type fanoutView struct {
	store reliable.Store
	peer  string
}

func (v *fanoutView) Put(string, []byte, []string) error {
	return errors.New("fanout: use atomic enqueue")
}
func (v *fanoutView) Get(id string) (eventstore.Event, error) {
	e, err := v.store.Get(id)
	if err != nil {
		return e, err
	}
	for _, target := range e.Targets {
		if target == v.peer {
			return v.project(e), nil
		}
	}
	return eventstore.Event{}, eventstore.ErrNotFound
}
func (v *fanoutView) Ack(id, target string) error {
	if target != v.peer {
		return reliable.ErrPeer
	}
	return v.store.Ack(id, target)
}
func (v *fanoutView) Pending(target string, limit int) ([]eventstore.Event, error) {
	if target != v.peer {
		return nil, reliable.ErrPeer
	}
	events, err := v.store.Pending(target, limit)
	if err != nil {
		return nil, err
	}
	for i := range events {
		events[i] = v.project(events[i])
	}
	return events, nil
}
func (v *fanoutView) PendingAfter(target string, cursor uint64, limit int) ([]eventstore.Event, uint64, error) {
	if target != v.peer {
		return nil, cursor, reliable.ErrPeer
	}
	events, next, err := v.store.PendingAfter(target, cursor, limit)
	if err != nil {
		return nil, cursor, err
	}
	for i := range events {
		events[i] = v.project(events[i])
	}
	return events, next, nil
}
func (v *fanoutView) Sequence() (uint64, error) { return v.store.Sequence() }
func (v *fanoutView) PendingUntil(target string, cursor, through uint64, limit int) ([]eventstore.Event, uint64, error) {
	if target != v.peer {
		return nil, cursor, reliable.ErrPeer
	}
	events, next, err := v.store.PendingUntil(target, cursor, through, limit)
	if err != nil {
		return nil, cursor, err
	}
	for i := range events {
		events[i] = v.project(events[i])
	}
	return events, next, nil
}
func (v *fanoutView) project(e eventstore.Event) eventstore.Event {
	e.Targets = []string{v.peer}
	confirmed := false
	for _, p := range e.Confirmed {
		if p == v.peer {
			confirmed = true
			break
		}
	}
	e.Confirmed = nil
	if confirmed {
		e.Confirmed = []string{v.peer}
	}
	return e
}
