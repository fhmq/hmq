package retained

import (
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/fhmq/hmq/v4/broker/storage"
)

func TestRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retain.log")
	s, err := Open(path)
	if errors.Is(err, storage.ErrUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	p := []byte("hello")
	if err = s.Put(Message{Topic: "a", QoS: 2, Payload: p}); err != nil {
		t.Fatal(err)
	}
	p[0] = 'x'
	if err = s.Put(Message{Topic: "b", Payload: []byte("bye")}); err != nil {
		t.Fatal(err)
	}
	if err = s.Delete("b"); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	m, ok := s.Get("a")
	if !ok || string(m.Payload) != "hello" || m.QoS != 2 {
		t.Fatalf("got %#v", m)
	}
	m.Payload[0] = 'x'
	m, _ = s.Get("a")
	if string(m.Payload) != "hello" {
		t.Fatal("payload alias")
	}
	if _, ok = s.Get("b"); ok {
		t.Fatal("deleted message recovered")
	}
}

func TestBadRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retain.log")
	l, err := storage.Open(path)
	if errors.Is(err, storage.ErrUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err = l.Append([]byte(`{"version":1,"message":{"topic":"a/+","qos":0}}`)); err != nil {
		t.Fatal(err)
	}
	l.Close()
	if _, err = Open(path); !errors.Is(err, ErrInvalid) {
		t.Fatalf("got %v", err)
	}
}

func TestReplayOverwriteDeleteAndInvalidRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retain.log")
	s, err := Open(path)
	if errors.Is(err, storage.ErrUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 200; i++ {
		if err := s.Put(Message{Topic: "a", Payload: []byte(fmt.Sprint(i))}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put(Message{Topic: "b", Payload: []byte("deleted")}); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("b"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if m, ok := s.Get("a"); !ok || string(m.Payload) != "199" {
		t.Fatalf("recovered: %#v", m)
	}
	if _, ok := s.Get("b"); ok {
		t.Fatal("deleted message recovered")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	l, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append([]byte(`{"version":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(path); !errors.Is(err, ErrInvalid) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("got %v", err)
	}
	l, err = storage.Open(path)
	if err != nil {
		t.Fatalf("log left locked: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRejectInvalidPayloadAndEncoding(t *testing.T) {
	for _, data := range [][]byte{
		[]byte(`{"version":1,"message":{"topic":"a","qos":0,"payload":""}}`),
		append(append([]byte(`{"version":1,"message":{"topic":"`), 0xff), []byte(`","payload":"eA=="}}`)...),
	} {
		path := filepath.Join(t.TempDir(), "retain.log")
		l, err := storage.Open(path)
		if errors.Is(err, storage.ErrUnsupported) {
			t.Skip(err)
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = l.Append(data); err != nil {
			t.Fatal(err)
		}
		if err = l.Close(); err != nil {
			t.Fatal(err)
		}
		if s, err := Open(path); !errors.Is(err, ErrInvalid) {
			if s != nil {
				s.Close()
			}
			t.Fatalf("expected invalid record, got %v", err)
		}
	}
	path := filepath.Join(t.TempDir(), "retain.log")
	s, err := Open(path)
	if errors.Is(err, storage.ErrUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Put(Message{Topic: "a"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("empty payload: %v", err)
	}
}

func TestConcurrent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retain.log")
	s, err := Open(path)
	if errors.Is(err, storage.ErrUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			topic := fmt.Sprint(i)
			for j := 0; j < 10; j++ {
				if e := s.Put(Message{Topic: topic, Payload: []byte("v")}); e != nil {
					t.Error(e)
				}
				s.Get(topic)
			}
		}(i)
	}
	wg.Wait()
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 16; i++ {
		if _, ok := s.Get(fmt.Sprint(i)); !ok {
			t.Fatal(i)
		}
	}
}
