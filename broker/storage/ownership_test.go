//go:build linux || darwin

package storage

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestExclusiveOwnership(t *testing.T) {
	if p := os.Getenv("STORAGE_LOCK_CHILD"); p != "" {
		l, err := Open(p)
		if l != nil {
			l.Close()
		}
		if !errors.Is(err, ErrLocked) {
			t.Fatalf("expected lock, got %v", err)
		}
		return
	}
	dir := t.TempDir()
	p := filepath.Join(dir, "log")
	l, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "alias")
	if err := os.Link(p, alias); err != nil {
		t.Fatal(err)
	}
	if other, err := Open(alias); !errors.Is(err, ErrLocked) {
		if other != nil {
			other.Close()
		}
		t.Fatalf("alias: %v", err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestExclusiveOwnership$")
	cmd.Env = append(os.Environ(), "STORAGE_LOCK_CHILD="+p)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v: %s", err, out)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	l, err = Open(alias)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
}
