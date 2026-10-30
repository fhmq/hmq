package storage

import (
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestReplay(t *testing.T) {
	l, err := Open(filepath.Join(t.TempDir(), "log"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for _, s := range []string{"one", "", "two"} {
		if err := l.Append([]byte(s)); err != nil {
			t.Fatal(err)
		}
	}
	var got [][]byte
	if err := l.Replay(func(b []byte) error { got = append(got, b); return nil }); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, [][]byte{[]byte("one"), {}, []byte("two")}) {
		t.Fatalf("records: %q", got)
	}
	stop := errors.New("stop")
	n := 0
	if err := l.Replay(func([]byte) error { n++; return stop }); err != stop || n != 1 {
		t.Fatalf("error %v, calls %d", err, n)
	}
	if err := l.Append([]byte("after")); err != nil {
		t.Fatal(err)
	}
}

func TestReplaySerializesClose(t *testing.T) {
	l, err := Open(filepath.Join(t.TempDir(), "log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append([]byte("record")); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	replayed := make(chan error, 1)
	go func() { replayed <- l.Replay(func([]byte) error { close(entered); <-release; return nil }) }()
	<-entered
	closed := make(chan error, 1)
	go func() { closed <- l.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("close during replay: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := <-replayed; err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if err := l.Replay(func([]byte) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
