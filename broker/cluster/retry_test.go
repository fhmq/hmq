package cluster

import (
	"math"
	"testing"
	"time"
)

func TestRetryBackoffAndJitter(t *testing.T) {
	base, max := time.Second, 8*time.Second
	current := base
	for _, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second} {
		if current != want {
			t.Fatalf("ceiling = %v, want %v", current, want)
		}
		low := retryDelay(current, func(int64) int64 { return 0 })
		high := retryDelay(current, func(n int64) int64 { return n - 1 })
		if low != current/2 || high != current {
			t.Fatalf("jitter range = [%v, %v], ceiling %v", low, high, current)
		}
		current = nextRetry(current, max)
	}
	if got := nextRetry(time.Duration(math.MaxInt64-1), time.Duration(math.MaxInt64)); got != time.Duration(math.MaxInt64) {
		t.Fatalf("overflow-safe ceiling = %v", got)
	}
}

func TestRetryWaitInterrupted(t *testing.T) {
	done := make(chan struct{})
	result := make(chan bool, 1)
	go func() { result <- waitRetry(done, time.Hour) }()
	close(done)
	select {
	case waited := <-result:
		if waited {
			t.Fatal("closed wait reported timer expiration")
		}
	case <-time.After(time.Second):
		t.Fatal("retry wait did not stop on close")
	}
}
