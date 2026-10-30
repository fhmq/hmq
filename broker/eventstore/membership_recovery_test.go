package eventstore

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestMembershipAndPartialAckSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events")
	s, err := Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Put("event", []byte("payload"), []string{"b", "a"}); err != nil {
		t.Fatal(err)
	}
	if err = s.Ack("event", "a"); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Put("event", []byte("payload"), []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err = s.Put("event", []byte("payload"), []string{"a", "c"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed membership: %v", err)
	}
	for _, tc := range []struct {
		target string
		count  int
	}{{"a", 0}, {"b", 1}, {"c", 0}} {
		pending, err := s.Pending(tc.target, 10)
		if err != nil || len(pending) != tc.count {
			t.Fatalf("pending %s: %+v, %v", tc.target, pending, err)
		}
	}
}
