package sessionstore

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRuntimeBatchesBoundWALAndFenceEpoch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.log")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	state := &State{Epoch: 4}
	for n := 0; n < 2000; n++ {
		state.Queue = append(state.Queue, Message{Topic: "a", Payload: make([]byte, 100), QoS: 1})
	}
	v, err := s.Put("client", 0, state)
	if err != nil {
		t.Fatal(err)
	}
	epoch := uint64(4)
	batches := []Batch{
		{ExpectedEpoch: &epoch, Enqueue: []Message{{Topic: "a", Payload: []byte("new"), QoS: 1}}},
		{ExpectedEpoch: &epoch, Start: map[uint16]Message{9: {Topic: "a", Payload: []byte("online"), QoS: 1}}},
		{ExpectedEpoch: &epoch, Acks: []Ack{{PacketID: 9, Type: "PUBACK"}}},
		{ExpectedEpoch: &epoch, Move: []uint16{10}},
	}
	for _, batch := range batches {
		before, _ := os.Stat(path)
		v, err = s.Apply("client", v, batch)
		if err != nil {
			t.Fatal(err)
		}
		after, _ := os.Stat(path)
		if delta := after.Size() - before.Size(); delta > 2048 {
			t.Fatalf("batch rewrote backlog: %d bytes", delta)
		}
	}
	next := uint64(5)
	v, err = s.Apply("client", v, Batch{NextEpoch: &next})
	if err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(path)
	if _, err := s.Apply("client", v, Batch{ExpectedEpoch: &epoch, Start: map[uint16]Message{11: {Topic: "a", QoS: 1}}}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale epoch: %v", err)
	}
	after, _ := os.Stat(path)
	if before.Size() != after.Size() {
		t.Fatal("rejected epoch wrote WAL")
	}
	s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	recovered, _, err := s.Get("client")
	if err != nil || recovered.Epoch != 5 || len(recovered.Queue) != 2000 || len(recovered.Inflight) != 1 {
		t.Fatalf("replay: %#v %v", recovered, err)
	}
}
