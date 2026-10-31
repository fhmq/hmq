package inbox

import (
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fhmq/hmq/v4/broker/eventstore"
)

func TestConsumerRecoveryAndRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inbox")
	s, err := Open(path, eventstore.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Accept("peer", "one", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, eventstore.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var attempts atomic.Int32
	delivered := make(chan Message, 1)
	c, err := NewConsumer(s, "peer", time.Millisecond, func(m Message) error {
		if attempts.Add(1) == 1 {
			return errors.New("temporary")
		}
		delivered <- m
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-delivered:
		if m.ID != "one" || string(m.Payload) != "payload" {
			t.Fatalf("unexpected message: %+v", m)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery timed out")
	}
	c.Close()
	c.Close()
	items, err := s.Pending("peer", 1)
	if err != nil || len(items) != 0 || attempts.Load() != 2 {
		t.Fatalf("pending=%v attempts=%d err=%v", items, attempts.Load(), err)
	}
}

func TestConsumerStopFromCallback(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "inbox"), eventstore.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Accept("peer", "id", nil); err != nil {
		t.Fatal(err)
	}
	ready := make(chan *Consumer, 1)
	c, err := NewConsumer(s, "peer", time.Millisecond, func(Message) error {
		(<-ready).Stop()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ready <- c
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("stop timed out")
	}
	items, err := s.Pending("peer", 1)
	if err != nil || len(items) != 0 {
		t.Fatalf("pending=%v err=%v", items, err)
	}
}

func TestConsumerCloseWaitsAndRetriesFailure(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "inbox"), eventstore.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Accept("peer", "id", nil); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	want := errors.New("delivery failed")
	c, err := NewConsumer(s, "peer", time.Millisecond, func(Message) error {
		close(entered)
		<-release
		return want
	})
	if err != nil {
		t.Fatal(err)
	}
	<-entered
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.Close() }()
	}
	c.Stop()
	select {
	case <-c.Done():
		t.Fatal("closed during delivery")
	default:
	}
	close(release)
	wg.Wait()
	if !errors.Is(c.Err(), want) {
		t.Fatalf("error: %v", c.Err())
	}
	items, err := s.Pending("peer", 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("pending=%v err=%v", items, err)
	}
	delivered := make(chan struct{}, 1)
	retry, err := NewConsumer(s, "peer", time.Millisecond, func(Message) error { delivered <- struct{}{}; return nil })
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-delivered:
	case <-time.After(time.Second):
		t.Fatal("retry timed out")
	}
	retry.Close()
	items, err = s.Pending("peer", 1)
	if err != nil || len(items) != 0 {
		t.Fatalf("pending=%v err=%v", items, err)
	}
}

type failingComplete struct {
	Queue
	calls atomic.Int32
}

func (q *failingComplete) Complete(peer, id string) error {
	if q.calls.Add(1) == 1 {
		return errors.New("completion failed")
	}
	return q.Queue.Complete(peer, id)
}

func TestConsumerRetriesCompletionByRedelivery(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "inbox"), eventstore.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Accept("peer", "id", nil); err != nil {
		t.Fatal(err)
	}
	q := &failingComplete{Queue: s}
	var count atomic.Int32
	c, err := NewConsumer(q, "peer", time.Millisecond, func(Message) error { count.Add(1); return nil })
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for q.calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	c.Close()
	if count.Load() != 2 {
		t.Fatalf("deliveries: %d", count.Load())
	}
	items, err := s.Pending("peer", 1)
	if err != nil || len(items) != 0 {
		t.Fatalf("pending=%v err=%v", items, err)
	}
}
