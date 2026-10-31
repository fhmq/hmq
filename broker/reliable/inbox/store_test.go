package inbox

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/fhmq/hmq/v4/broker/eventstore"
	"github.com/fhmq/hmq/v4/broker/reliable"
	"github.com/fhmq/hmq/v4/broker/storage"
)

func TestCapacityRetainsCompletedIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.log")
	s, err := Open(path, eventstore.Limits{MaxEvents: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Accept("a", "id", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err = s.Complete("a", "id"); err != nil {
		t.Fatal(err)
	}
	if err = s.Accept("a", "id", []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err = s.Accept("a", "id", []byte("two")); !errors.Is(err, reliable.ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	if err = s.Accept("b", "id", nil); !errors.Is(err, eventstore.ErrCapacity) {
		t.Fatalf("capacity: %v", err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, eventstore.Limits{MaxEvents: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Accept("b", "id", nil); !errors.Is(err, eventstore.ErrCapacity) {
		t.Fatalf("recovered capacity: %v", err)
	}
}

func TestIncompleteLogFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.log")
	s, err := Open(path, eventstore.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Accept("peer", "id", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte("HMQ")); err != nil {
		t.Fatal(err)
	}
	if err = f.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err = Open(path, eventstore.Limits{}); !errors.Is(err, storage.ErrCorrupt) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("open incomplete log: %v", err)
	}
}

func TestRecoveryDedupAndCompletion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox.log")
	s, err := Open(path, eventstore.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- s.Accept("peer.a", "id/900", []byte("payload")) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Accept("peer.a", "id/900", []byte("other")); !errors.Is(err, reliable.ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	if err := s.Accept("peer.b", "id/900", []byte("other")); err != nil {
		t.Fatal(err)
	}
	if err := s.Accept("peer.a", "id/2", []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := s.Complete("peer.a", "id/900"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, eventstore.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	items, err := s.Pending("peer.a", 10)
	if err != nil || len(items) != 1 || items[0].ID != "id/2" || string(items[0].Payload) != "second" {
		t.Fatalf("pending: %+v, %v", items, err)
	}
	items, err = s.Pending("peer.b", 10)
	if err != nil || len(items) != 1 || string(items[0].Payload) != "other" {
		t.Fatalf("peer b: %+v, %v", items, err)
	}
	if err := s.Accept("peer.a", "id/900", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := s.Accept("peer.a", "id/900", []byte("changed")); !errors.Is(err, reliable.ErrConflict) {
		t.Fatalf("completed conflict: %v", err)
	}
}
