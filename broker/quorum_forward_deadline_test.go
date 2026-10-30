package broker

import (
	"context"
	"errors"
	"testing"
	"time"
)

// Models the interval after the deadline but before a timer callback updates Err.
type laggingQuorumContext struct {
	context.Context
	deadline time.Time
}

func (c laggingQuorumContext) Deadline() (time.Time, bool) { return c.deadline, true }

func TestQuorumForwardRejectsElapsedUnnotifiedDeadline(t *testing.T) {
	ctx := laggingQuorumContext{Context: context.Background(), deadline: time.Now().Add(-time.Second)}
	if ctx.Err() != nil {
		t.Fatal("test context must not yet be notified")
	}
	if err := quorumForwardContextError(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("elapsed budget accepted: %v", err)
	}
	ctx.deadline = time.Now().Add(time.Hour)
	if err := quorumForwardContextError(ctx); err != nil {
		t.Fatalf("unexpired budget rejected: %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	ctx.Context = cancelled
	if err := quorumForwardContextError(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation cause lost: %v", err)
	}
}
