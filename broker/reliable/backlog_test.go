package reliable

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestResumeDoesNotOvertakeDurableBacklog(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "events"))
	defer s.Close()
	for i := 0; i <= recoveryBatch; i++ {
		if err := s.Put(fmt.Sprintf("old-%03d", i), nil, []string{"peer"}); err != nil {
			t.Fatal(err)
		}
	}
	// No worker: explicitly place the new enqueue between ACK and recovery.
	l := &Link{peer: "peer", sender: &testSender{}, store: s, maxFrameSize: DefaultMaxFrameSize, pending: make(map[string]struct{}), done: make(chan struct{}), wake: make(chan struct{}, 1)}
	if err := l.loadPendingLocked(); err != nil {
		t.Fatal(err)
	}
	if err := l.Resume(fmt.Sprintf("old-%03d", recoveryBatch)); err != nil {
		t.Fatal(err)
	}
	if err := l.loadPendingLocked(); err != nil {
		t.Fatal(err)
	}
	if l.Pending(fmt.Sprintf("old-%03d", recoveryBatch)) {
		t.Fatal("full unacknowledged window must block later events")
	}
	if err := l.Handle("peer", wire("ack", "old-000"), nil); err != nil {
		t.Fatal(err)
	}
	if err := l.Enqueue("new", nil); err != nil {
		t.Fatal(err)
	}
	if !l.Pending(fmt.Sprintf("old-%03d", recoveryBatch)) || l.Pending("new") {
		t.Fatal("new event overtook older durable backlog")
	}
	if len(l.pending) != recoveryBatch {
		t.Fatalf("pending = %d", len(l.pending))
	}
	if err := l.Handle("peer", wire("ack", "old-001"), nil); err != nil {
		t.Fatal(err)
	}
	if err := l.Resume("new"); err != nil {
		t.Fatal(err)
	}
	if !l.Pending("new") || len(l.pending) != recoveryBatch {
		t.Fatal("new event not scheduled after backlog cleared")
	}
}

func TestConcurrentBacklogBoundedAndRecovered(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events")
	s := open(t, path)
	sender := &testSender{fail: true}
	l, err := New("peer", sender, s, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	const total = recoveryBatch*2 + 31
	var wg sync.WaitGroup
	errs := make(chan error, total)
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("id-%04d", i)
			if err := l.Enqueue(id, []byte(id)); err != nil {
				errs <- err
				return
			}
			if err := l.Resume(id); err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	l.mu.Lock()
	count := len(l.pending)
	l.mu.Unlock()
	if count > recoveryBatch {
		t.Fatalf("pending = %d", count)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path)
	defer s.Close()
	sender = &testSender{}
	l, err = New("peer", sender, s, 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	seen := make(map[string]bool)
	index := 0
	deadline := time.Now().Add(2 * time.Minute)
	for len(seen) < total && time.Now().Before(deadline) {
		sender.mu.Lock()
		frames := append([][]byte(nil), sender.frames[index:]...)
		index = len(sender.frames)
		sender.mu.Unlock()
		for _, data := range frames {
			var f frame
			if err := json.Unmarshal(data, &f); err != nil {
				t.Fatal(err)
			}
			if !seen[f.ID] {
				if err := l.Handle("peer", wire("ack", f.ID), nil); err != nil {
					t.Fatal(err)
				}
				seen[f.ID] = true
			}
		}
		l.mu.Lock()
		count = len(l.pending)
		l.mu.Unlock()
		if count > recoveryBatch {
			t.Fatalf("pending = %d", count)
		}
		time.Sleep(time.Millisecond)
	}
	if len(seen) != total {
		t.Fatalf("delivered %d of %d", len(seen), total)
	}
}

func TestRoundRobinWithoutFirstBatchACK(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events")
	s := open(t, path)
	for i := 0; i < recoveryBatch+3; i++ {
		if err := s.Put(fmt.Sprintf("id-%03d", i), nil, []string{"peer"}); err != nil {
			t.Fatal(err)
		}
	}
	sender := &testSender{}
	l, err := New("peer", sender, s, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[string]int)
	index := 0
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && (seen["id-000"] < 2 || seen["id-258"] == 0) {
		sender.mu.Lock()
		frames := append([][]byte(nil), sender.frames[index:]...)
		index = len(sender.frames)
		sender.mu.Unlock()
		for _, data := range frames {
			var f frame
			if err := json.Unmarshal(data, &f); err != nil {
				t.Fatal(err)
			}
			seen[f.ID]++
			if f.ID >= "id-256" {
				if err := l.Handle("peer", wire("ack", f.ID), nil); err != nil {
					t.Fatal(err)
				}
			}
		}
		l.mu.Lock()
		if len(l.pending) > recoveryBatch {
			t.Fatal("retry batch exceeded bound")
		}
		l.mu.Unlock()
		time.Sleep(time.Millisecond)
	}
	if seen["id-000"] < 2 || seen["id-258"] == 0 {
		t.Fatalf("rotation did not progress: first=%d last=%d", seen["id-000"], seen["id-258"])
	}
	if err := l.Handle("peer", wire("ack", "id-256"), nil); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path)
	defer s.Close()
	for _, id := range []string{"id-256", "id-257", "id-258"} {
		e, err := s.Get(id)
		if err != nil || len(e.Confirmed) != 1 {
			t.Fatalf("ACK not recovered for %s: %+v, %v", id, e, err)
		}
	}
	l, err = New("peer", &testSender{}, s, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Handle("peer", wire("ack", "id-256"), nil); err != nil {
		t.Fatal(err)
	}
	if err := l.Handle("peer", wire("ack", "id-000"), nil); err != nil {
		t.Fatal(err)
	}
}

func TestRoundRobinWithContinuousEnqueue(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "events"))
	defer s.Close()
	for i := 0; i <= recoveryBatch; i++ {
		if err := s.Put(fmt.Sprintf("old-%03d", i), nil, []string{"peer"}); err != nil {
			t.Fatal(err)
		}
	}
	l := &Link{peer: "peer", store: s, maxFrameSize: DefaultMaxFrameSize, pending: make(map[string]struct{})}
	for batch := 0; batch < 3; batch++ {
		if err := l.rotatePendingLocked(); err != nil {
			t.Fatal(err)
		}
		switch batch {
		case 0, 2:
			if _, ok := l.pending["old-000"]; !ok {
				t.Fatalf("old event not retried in batch %d", batch)
			}
		case 1:
			if _, ok := l.pending["old-256"]; !ok {
				t.Fatal("second batch not reached")
			}
			if len(l.pending) != 1 {
				t.Fatalf("new events entered the fixed cycle: %v", l.pending)
			}
		}
		if err := s.Put(fmt.Sprintf("new-%03d", batch), nil, []string{"peer"}); err != nil {
			t.Fatal(err)
		}
	}
}
