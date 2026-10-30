package outbox

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestInvalidPeerCannotCollideWithReplacementCharacter(t *testing.T) {
	p := filepath.Join(t.TempDir(), "outbox")
	if _, err := Open(p, string([]byte{0xff}), 10, 100); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid peer: %v", err)
	}
	o, err := Open(p, "\ufffd", 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	id := o.Incarnation()
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o, err = Open(p, "\ufffd", 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if o.Incarnation() != id {
		t.Fatal("incarnation changed on reopen")
	}
}
