package eventstore

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestBindIdentityCapacityDoesNotCorruptRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events")
	limits := Limits{MaxBytes: 1}
	s, err := Open(path, limits)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.BindIdentity([]byte("owner")); !errors.Is(err, ErrCapacity) {
		t.Fatalf("BindIdentity: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, limits)
	if err != nil {
		t.Fatalf("reopen after rejected binding: %v", err)
	}
	defer s.Close()
}
