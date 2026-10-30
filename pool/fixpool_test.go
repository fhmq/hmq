package pool

import (
	"sync"
	"testing"
	"time"
)

func TestCloseWaitsForRunningTask(t *testing.T) {
	p := New(1)
	started := make(chan struct{})
	release := make(chan struct{})
	if !p.Submit("client", func() { close(started); <-release }) {
		t.Fatal("submit failed")
	}
	<-started
	done := make(chan struct{})
	go func() { p.Close(); close(done) }()
	select {
	case <-done:
		t.Fatal("close returned while task was running")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	<-done
	if p.Submit("client", func() {}) {
		t.Fatal("submit succeeded after close")
	}
}

func TestConcurrentClose(t *testing.T) {
	p := New(2)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); p.Close() }()
	}
	wg.Wait()
}

func TestCloseDrainsAcceptedTasks(t *testing.T) {
	p := New(1)
	started := make(chan struct{})
	release := make(chan struct{})
	p.Submit("client", func() { close(started); <-release })
	<-started
	var mu sync.Mutex
	completed := 0
	for i := 0; i < 100; i++ {
		if !p.Submit("client", func() { mu.Lock(); completed++; mu.Unlock() }) {
			t.Fatal("submit failed")
		}
	}
	done := make(chan struct{})
	go func() { p.Close(); close(done) }()
	close(release)
	<-done
	mu.Lock()
	defer mu.Unlock()
	if completed != 100 {
		t.Fatalf("completed %d tasks, want 100", completed)
	}
}

func TestCloseUnblocksSubmitWhenQueueIsFull(t *testing.T) {
	p := New(1)
	started := make(chan struct{})
	release := make(chan struct{})
	p.Submit("client", func() { close(started); <-release })
	<-started
	for i := 0; i < cap(p.taskQueue[0]); i++ {
		if !p.Submit("client", func() {}) {
			t.Fatal("failed to fill queue")
		}
	}
	result := make(chan bool, 1)
	go func() { result <- p.Submit("client", func() {}) }()
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	select {
	case accepted := <-result:
		if accepted {
			t.Fatal("blocked submit accepted after close")
		}
	case <-time.After(time.Second):
		t.Fatal("submit remained blocked after close")
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close did not finish")
	}
}
