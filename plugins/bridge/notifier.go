package bridge

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrNotifierFull    = errors.New("bridge notification queue full")
	ErrNotifierStopped = errors.New("bridge notifier stopped")
)

// Notifier delivers best-effort notifications serially. The handler must return
// promptly when its context is canceled; Go cannot forcibly stop a blocked call.
type Notifier struct {
	mu      sync.Mutex
	stopped bool
	queue   chan Elements
	cancel  context.CancelFunc
	done    chan struct{}
}

// NewNotifier starts one worker with room for capacity waiting notifications.
// The handler must be non-nil and capacity must be positive.
func NewNotifier(capacity int, handler func(context.Context, Elements)) *Notifier {
	if capacity <= 0 || handler == nil {
		panic("bridge: invalid notifier configuration")
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Notifier{queue: make(chan Elements, capacity), cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(n.done)
		for {
			select {
			case <-ctx.Done():
				return
			case e := <-n.queue:
				if ctx.Err() != nil {
					return
				}
				handler(ctx, e)
			}
		}
	}()
	return n
}

// Notify copies e and never waits for queue space or the handler.
func (n *Notifier) Notify(e Elements) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return ErrNotifierStopped
	}
	select {
	case n.queue <- e:
		return nil
	default:
		return ErrNotifierFull
	}
}

// Stop rejects future notifications and cancels the active handler. It does not
// wait; Done closes after the handler returns and the worker exits.
func (n *Notifier) Stop() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.stopped {
		n.stopped = true
		n.cancel()
	}
}

func (n *Notifier) Done() <-chan struct{} { return n.done }
