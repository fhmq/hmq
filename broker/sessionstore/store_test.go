package sessionstore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/fhmq/hmq/v3/broker/storage"
)

func TestRecoveryAndIsolation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	in := &State{Subscriptions: map[string]byte{"a/#": 2}, Queue: []Message{{Topic: "a/b", Payload: []byte("data"), QoS: 1}}}
	v, err := s.Put("client", 0, in)
	if err != nil {
		t.Fatal(err)
	}
	in.Queue[0].Payload[0] = 'x'
	in.Subscriptions["a/#"] = 0
	got, _, _ := s.Get("client")
	if string(got.Queue[0].Payload) != "data" || got.Subscriptions["a/#"] != 2 {
		t.Fatal(got)
	}
	got.Queue[0].Payload[0] = 'z'
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, version, err := s.Get("client")
	if err != nil || version != v || string(got.Queue[0].Payload) != "data" {
		t.Fatalf("%+v %d %v", got, version, err)
	}
	v2, err := s.Put("client", v, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put("client", v, in); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	got, version, _ = s.Get("client")
	if got != nil || version != v2 {
		t.Fatal(got, version)
	}
}

func TestUTF8AndBinaryRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	bad := string([]byte{0xff})
	for _, tc := range []struct {
		id    string
		state *State
	}{
		{bad, &State{}},
		{"c", &State{Subscriptions: map[string]byte{bad: 1}}},
		{"c", &State{Queue: []Message{{Topic: bad, QoS: 1}}}},
	} {
		if _, err := s.Put(tc.id, 0, tc.state); !errors.Is(err, ErrInvalid) {
			t.Fatalf("expected invalid: %v", err)
		}
	}
	if _, _, err := s.Get(bad); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	v, err := s.Put("客户端", 0, &State{Subscriptions: map[string]byte{"主题/#": 2}})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte{0, 0xff, 0xfe, 0x80}
	v, err = s.Apply("客户端", v, Batch{Enqueue: []Message{{Topic: "主题/a", QoS: 2, Payload: payload}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply("客户端", v, Batch{RemoveSubscriptions: []string{bad}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, version, err := s.Get("客户端")
	if err != nil || version != v || got.Subscriptions["主题/#"] != 2 || !bytes.Equal(got.Queue[0].Payload, payload) {
		t.Fatalf("%+v %d %v", got, version, err)
	}
}

func TestIncrementalBacklogRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.Put("c", 0, &State{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 9; i++ {
		v, err = s.Apply("c", v, Batch{Enqueue: []Message{{Topic: "a", QoS: 1, Payload: bytes.Repeat([]byte{byte(i)}, 2<<20)}}})
		if err != nil {
			t.Fatal(err)
		}
	}
	v, err = s.Apply("c", v, Batch{Remove: 1, SetSubscriptions: map[string]byte{"a/#": 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply("c", v-1, Batch{}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, version, err := s.Get("c")
	if err != nil || version != v || len(got.Queue) != 8 || got.Queue[0].Payload[0] != 1 || got.Subscriptions["a/#"] != 1 {
		t.Fatalf("version=%d err=%v", version, err)
	}
}

func TestRejectInvalidUTF8Log(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	l, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	record := append([]byte(`{"format":1,"sequence":1,"client_id":"`), 0xff)
	record = append(record, []byte(`","state":{}}`)...)
	if err := l.Append(record); err != nil {
		t.Fatal(err)
	}
	l.Close()
	if _, err := Open(path); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestConcurrentCAS(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "log"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const n = 32
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := s.Put("c", 0, &State{}); errs <- err }()
	}
	wg.Wait()
	close(errs)
	wins := 0
	for err := range errs {
		if err == nil {
			wins++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	if wins != 1 {
		t.Fatal(wins)
	}
}

func TestCorruptAndInvalid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put("c", 0, &State{Queue: []Message{{Topic: "t", QoS: 0}}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := s.Put("c", 0, &State{}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte("broken")); err != nil {
		t.Fatal(err)
	}
	f.Close()
	if _, err := Open(path); !errors.Is(err, storage.ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestInvalidRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	l, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append([]byte(`{"format":1,"sequence":2,"client_id":"c","state":{}}`)); err != nil {
		t.Fatal(err)
	}
	l.Close()
	if _, err := Open(path); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}
