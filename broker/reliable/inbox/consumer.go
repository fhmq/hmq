package inbox

import (
	"errors"
	"sync"
	"time"
)

// Queue is the durable portion of an inbox used by a Consumer.
type Queue interface {
	Pending(peer string, limit int) ([]Message, error)
	Complete(peer, id string) error
}

// Consumer serially delivers pending messages for one peer. Only one consumer
// should own a given peer; the queue does not provide cross-consumer claims.
type Consumer struct {
	stop chan struct{}
	done chan struct{}
	once sync.Once
	mu   sync.Mutex
	err  error
}

// NewConsumer starts recovery and polling. interval must be positive. A failed
// delivery or completion is retried after interval, before later messages.
// The callback must return for Close to finish.
func NewConsumer(queue Queue, peer string, interval time.Duration, deliver func(Message) error) (*Consumer, error) {
	if queue == nil || peer == "" || interval <= 0 || deliver == nil {
		return nil, errors.New("inbox: invalid consumer configuration")
	}
	c := &Consumer{stop: make(chan struct{}), done: make(chan struct{})}
	go c.run(queue, peer, interval, deliver)
	return c, nil
}

func (c *Consumer) run(queue Queue, peer string, interval time.Duration, deliver func(Message) error) {
	defer close(c.done)
	for {
		select {
		case <-c.stop:
			return
		default:
		}
		messages, err := queue.Pending(peer, 1)
		if err == nil && len(messages) != 0 {
			select {
			case <-c.stop:
				return
			default:
			}
			err = deliver(messages[0])
			if err == nil {
				err = queue.Complete(peer, messages[0].ID)
			}
		}
		c.mu.Lock()
		c.err = err
		c.mu.Unlock()
		if err == nil && len(messages) != 0 {
			continue
		}
		timer := time.NewTimer(interval)
		select {
		case <-c.stop:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// Err reports the most recent attempt's error, cleared by a successful poll.
func (c *Consumer) Err() error { c.mu.Lock(); defer c.mu.Unlock(); return c.err }

// Stop requests shutdown without waiting. It is safe to call from deliver.
// A delivery already started is completed (including Complete on success).
func (c *Consumer) Stop() { c.once.Do(func() { close(c.stop) }) }

// Done is closed when polling and any in-flight delivery have finished.
func (c *Consumer) Done() <-chan struct{} { return c.done }

// Close stops polling and waits for the current delivery and completion. It
// does not close the queue. Concurrent and repeated calls are safe. A delivery
// callback must use Stop instead of Close to avoid waiting for itself.
func (c *Consumer) Close() { c.Stop(); <-c.done }
