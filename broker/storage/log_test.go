package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestRecovery(t *testing.T) {
	p := filepath.Join(t.TempDir(), "log")
	l, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	if err = l.Append([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err = l.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte("HMQ")); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err = Open(p); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected incomplete header, got %v", err)
	}
	info, err := os.Stat(p)
	if err != nil || info.Size() != headerSize+5+3 {
		t.Fatalf("size: %v %v", info, err)
	}
	if err = os.Truncate(p, headerSize+5); err != nil {
		t.Fatal(err)
	}
	f, err = os.OpenFile(p, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.WriteAt([]byte("X"), headerSize); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err = Open(p); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected corruption, got %v", err)
	}
}

func TestPartialPayloadAndBounds(t *testing.T) {
	p := filepath.Join(t.TempDir(), "log")
	l, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	if err = l.Append(make([]byte, MaxRecordSize+1)); !errors.Is(err, ErrTooLarge) {
		t.Fatal(err)
	}
	if err = l.Append([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	l.Close()
	if err = os.Truncate(p, headerSize+2); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(p); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("expected incomplete payload, got %v", err)
	}
}

func TestIncompleteAndDamagedLengthPreserveFile(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{"partial header", func(b []byte) []byte { return b[:headerSize+3+5] }},
		{"partial payload", func(b []byte) []byte { return b[:len(b)-1] }},
		{"damaged length", func(b []byte) []byte { binary.BigEndian.PutUint32(b[8:12], 1000); return b }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "log")
			l, err := Open(p)
			if err != nil {
				t.Fatal(err)
			}
			if err = l.Append([]byte("one")); err != nil {
				t.Fatal(err)
			}
			if err = l.Append([]byte("second")); err != nil {
				t.Fatal(err)
			}
			if err = l.Close(); err != nil {
				t.Fatal(err)
			}
			b, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			b = tc.mutate(b)
			if err = os.WriteFile(p, b, 0600); err != nil {
				t.Fatal(err)
			}
			if _, err = Open(p); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("expected corruption: %v", err)
			}
			after, err := os.ReadFile(p)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(b, after) {
				t.Fatal("Open changed corrupt log")
			}
		})
	}
}

func TestConcurrentAndClose(t *testing.T) {
	l, err := Open(filepath.Join(t.TempDir(), "log"))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.Append([]byte("x")); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	r, err := l.ReadAll()
	if err != nil || len(r) != 20 {
		t.Fatalf("%d %v", len(r), err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Append(nil); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := l.ReadAll(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := l.Close(); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}

func TestWriteFailurePoisonsUntilReopen(t *testing.T) {
	p := filepath.Join(t.TempDir(), "log")
	l, err := Open(p)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append([]byte("committed")); err != nil {
		t.Fatal(err)
	}
	// Simulate an I/O failure without changing the durable file.
	if err := l.f.Close(); err != nil {
		t.Fatal(err)
	}
	first := l.Append([]byte("uncertain"))
	if first == nil {
		t.Fatal("expected write failure")
	}
	if err := l.Append([]byte("later")); err != first {
		t.Fatalf("append after failure: %v", err)
	}
	if _, err := l.ReadAll(); err != first {
		t.Fatalf("read after failure: %v", err)
	}
	_ = l.Close()
	l, err = Open(p)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	records, err := l.ReadAll()
	if err != nil || len(records) != 1 || string(records[0]) != "committed" {
		t.Fatalf("recovery: %q, %v", records, err)
	}
}
