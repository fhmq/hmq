package inbox

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestInvalidSenderCannotCollideWithReplacementCharacter(t *testing.T) {
	p := filepath.Join(t.TempDir(), "inbox")
	if _, err := Open(p, string([]byte{0xff})); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid sender: %v", err)
	}
	i, err := Open(p, "\ufffd")
	if err != nil {
		t.Fatal(err)
	}
	if err := i.Close(); err != nil {
		t.Fatal(err)
	}
	i, err = Open(p, "\ufffd")
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
}
