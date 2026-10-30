package bridge

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestNotifierBoundedAndStop(t *testing.T) {
	started := make(chan struct{})
	n := NewNotifier(1, func(ctx context.Context, e Elements) {
		close(started)
		<-ctx.Done()
	})
	if err := n.Notify(Elements{Action: Connect}); err != nil {
		t.Fatal(err)
	}
	<-started
	if err := n.Notify(Elements{Action: Disconnect}); err != nil {
		t.Fatal(err)
	}
	if err := n.Notify(Elements{}); !errors.Is(err, ErrNotifierFull) {
		t.Fatalf("got %v", err)
	}
	n.Stop()
	n.Stop()
	if err := n.Notify(Elements{}); !errors.Is(err, ErrNotifierStopped) {
		t.Fatalf("got %v", err)
	}
	select {
	case <-n.Done():
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}

func TestNotifierConcurrentStop(t *testing.T) {
	n := NewNotifier(4, func(context.Context, Elements) {})
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = n.Notify(Elements{})
			}
			n.Stop()
		}()
	}
	wg.Wait()
	select {
	case <-n.Done():
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
}
