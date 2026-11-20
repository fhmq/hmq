package sessionstore

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestInboundQoS2WALRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.wal")
	open := func() *Store {
		t.Helper()
		s, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := open()
	if _, err := s.Put("pub", 0, &State{Epoch: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put("sub", 0, &State{Epoch: 2, Subscriptions: map[string]byte{"foo/#": 2, "+": 1}}); err != nil {
		t.Fatal(err)
	}
	m := Message{Topic: "foo", Payload: []byte("first"), QoS: 2}
	if err := s.Receive("pub", 1, 7, m, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open()
	m.Payload = []byte("second")
	if err := s.Receive("pub", 1, 7, m, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.Receive("pub", 1, 8, m, 1); !errors.Is(err, ErrInvalid) {
		t.Fatalf("receive at capacity: %v", err)
	}
	if _, _, err := s.Release("pub", 9, 7, time.Now()); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale epoch: %v", err)
	}
	if got, found, err := s.Release("pub", 1, 7, time.Now()); err != nil || !found || string(got.Payload) != "first" {
		t.Fatalf("release: %#v %v %v", got, found, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open()
	defer s.Close()
	if _, found, err := s.Release("pub", 1, 7, time.Now()); err != nil || found {
		t.Fatalf("duplicate release: %v %v", found, err)
	}
	sub, _, err := s.Get("sub")
	if err != nil || len(sub.Queue) != 1 || sub.Queue[0].QoS != 2 || string(sub.Queue[0].Payload) != "first" {
		t.Fatalf("recovered queue: %#v %v", sub, err)
	}
}
