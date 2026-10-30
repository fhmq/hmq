package broker

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/fhmq/hmq/v3/broker/eventstore"
	"github.com/fhmq/hmq/v3/broker/reliable"
)

type fanoutSender struct{ sent chan string }

type mutatingFanoutStore struct {
	reliable.Store
	targets [][]string
}

func (s *mutatingFanoutStore) Put(_ string, _ []byte, targets []string) error {
	s.targets = append(s.targets, append([]string(nil), targets...))
	targets[0] = "changed"
	return errors.New("append failed")
}

func TestFanoutStoreCannotMutatePeersOnFailure(t *testing.T) {
	s := &mutatingFanoutStore{}
	f, err := newFanoutStore(s, []string{"a", "b"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := f.enqueue("id", nil); err == nil {
			t.Fatal("expected failure")
		}
	}
	if s.targets[1][0] != "a" || s.targets[1][1] != "b" {
		t.Fatalf("targets changed: %v", s.targets)
	}
	if _, err := f.view("a"); err != nil {
		t.Fatal(err)
	}
}

func (s fanoutSender) Send(peer string, _ []byte) error { s.sent <- peer; return nil }

func TestFanoutRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events")
	s, err := eventstore.Open(path, eventstore.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	f, err := newFanoutStore(s, []string{"a", "b"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = f.enqueue("id", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err = s.Ack("id", "a"); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = eventstore.Open(path, eventstore.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f, err = newFanoutStore(s, []string{"a", "b"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	view, err := f.view("b")
	if err != nil {
		t.Fatal(err)
	}
	e, err := view.Get("id")
	if err != nil || len(e.Targets) != 1 || e.Targets[0] != "b" || len(e.Confirmed) != 0 {
		t.Fatalf("projection: %+v %v", e, err)
	}
	sender := fanoutSender{make(chan string, 10)}
	link, err := reliable.New("b", sender, view, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer link.Close()
	select {
	case peer := <-sender.sent:
		if peer != "b" {
			t.Fatal(peer)
		}
	case <-time.After(time.Second):
		t.Fatal("recovered event not sent")
	}
	if err := view.Ack("id", "a"); !errors.Is(err, reliable.ErrPeer) {
		t.Fatal(err)
	}
	if err := view.Put("other", nil, []string{"b"}); err == nil {
		t.Fatal("view accepted non-atomic write")
	}
}

func TestFanoutPreflightAndAtomicPut(t *testing.T) {
	s, err := eventstore.Open(filepath.Join(t.TempDir(), "events"), eventstore.Limits{MaxEvents: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	f, err := newFanoutStore(s, []string{"a", "b"}, 50)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.enqueue("large", make([]byte, 100)); !errors.Is(err, reliable.ErrFrameTooLarge) {
		t.Fatal(err)
	}
	if _, err := s.Get("large"); !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatal(err)
	}
	if err := f.enqueue("ok", nil); err != nil {
		t.Fatal(err)
	}
	e, err := s.Get("ok")
	if err != nil || len(e.Targets) != 2 {
		t.Fatalf("event: %+v %v", e, err)
	}
	if err := f.enqueue("full", nil); !errors.Is(err, eventstore.ErrCapacity) {
		t.Fatal(err)
	}
	if _, err := s.Get("full"); !errors.Is(err, eventstore.ErrNotFound) {
		t.Fatal(err)
	}
}
