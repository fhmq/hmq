package replication

import (
	"errors"
	"testing"
)

func TestPartitionPreflightRejectsKnownFailures(t *testing.T) {
	// Only the retained count and the known-ID entry matter to this check.
	p := &partition{entries: make([]Entry, maxPartitionEntries), ids: map[string]int{"known": 0}}
	p.entries[0] = Entry{Index: 7, ID: "known", Payload: []byte("same")}
	if err := p.preflight(command{"new", nil}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("full partition accepted: %v", err)
	}
	if err := p.preflight(command{"known", []byte("different")}); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict accepted: %v", err)
	}
	if err := p.preflight(command{"known", []byte("same")}); err != nil {
		t.Fatalf("duplicate incorrectly rejected at capacity: %v", err)
	}
	p = &partition{bytes: maxPartitionBytes}
	if err := p.preflight(command{"new", nil}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("byte capacity ignored: %v", err)
	}
}
