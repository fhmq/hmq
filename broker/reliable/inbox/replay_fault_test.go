package inbox

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/fhmq/hmq/v3/broker/eventstore"
	"github.com/fhmq/hmq/v3/broker/reliable"
)

func TestReplayAcrossAcceptanceAndCompletionRestarts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox")
	open := func() *Store {
		s, err := Open(path, eventstore.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	s := open()
	if err := s.Accept("peer", "id", []byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open()
	if err := s.Accept("peer", "id", []byte("data")); err != nil {
		t.Fatal(err)
	}
	items, err := s.Pending("peer", 10)
	if err != nil || len(items) != 1 || items[0].ID != "id" || string(items[0].Payload) != "data" {
		t.Fatalf("pending: %+v, %v", items, err)
	}
	if err := s.Complete("peer", "id"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open()
	defer s.Close()
	if err := s.Accept("peer", "id", []byte("data")); err != nil {
		t.Fatal(err)
	}
	if err := s.Accept("peer", "id", []byte("different")); !errors.Is(err, reliable.ErrConflict) {
		t.Fatalf("conflicting replay: %v", err)
	}
	items, err = s.Pending("peer", 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("completed replay: %+v, %v", items, err)
	}
}
