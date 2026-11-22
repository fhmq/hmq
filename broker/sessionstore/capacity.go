package sessionstore

import (
	"errors"
	"time"
)

var ErrQuota = errors.New("sessionstore: pending message quota exceeded")

// Limits are per session. A zero dimension is unlimited.
type Limits struct {
	MaxPendingMessages int   `json:"maxPendingMessages,omitempty"`
	MaxPendingBytes    int64 `json:"maxPendingBytes,omitempty"`
}

func (l Limits) Enabled() bool { return l != (Limits{}) }
func (l Limits) Valid() bool   { return l.MaxPendingMessages >= 0 && l.MaxPendingBytes >= 0 }

type Usage struct {
	Messages int   `json:"messages"`
	Bytes    int64 `json:"bytes"`
}

type Capacity struct {
	Sessions       int        `json:"sessions"`
	Queued         Usage      `json:"queued"`
	Outbound       Usage      `json:"outboundInflight"`
	InboundQoS2    Usage      `json:"inboundQos2"`
	OldestQueuedAt *time.Time `json:"oldestQueuedAt,omitempty"`
}

func messageBytes(m Message) int64 {
	// Charge topic, payload and serialized MQTT 5 metadata, not Go heap
	// overhead or the encoded WAL record.
	size, err := messageReadBytes(m, int(^uint(0)>>1))
	if err != nil {
		return int64(len(m.Topic)) + int64(len(m.Payload))
	}
	return int64(len(m.Topic)) + int64(size)
}

func (u *Usage) add(message Message) {
	u.Messages++
	u.Bytes += messageBytes(message)
}

func (u *Usage) remove(message Message) {
	u.Messages--
	u.Bytes -= messageBytes(message)
}

func pending(state *State) (usage Usage) {
	if state == nil {
		return usage
	}
	for _, m := range state.Queue {
		usage.add(m)
	}
	for _, m := range state.Inflight {
		usage.add(m.Message)
	}
	for _, m := range state.Inbound {
		usage.add(m)
	}
	return usage
}

func pendingAfterBatch(e entry, next *State, batch *Batch) Usage {
	usage := e.pending
	for _, m := range e.state.Queue[:batch.Remove] {
		usage.remove(m)
	}
	for _, m := range batch.Enqueue {
		usage.add(m)
	}
	for _, m := range batch.Start {
		usage.add(m)
	}
	for _, m := range batch.Receive {
		usage.add(m)
	}
	if len(batch.Acks) != 0 {
		removed := make(map[uint16]bool, len(batch.Acks))
		for _, ack := range batch.Acks {
			id := ack.PacketID
			if prev, ok := e.state.Inflight[id]; ok && !removed[id] {
				if current, present := next.Inflight[id]; !present || current.Sequence != prev.Sequence {
					usage.remove(prev.Message)
					removed[id] = true
				}
			}
		}
	}
	return usage
}

// A lower limit must not strand already durable messages. Allow transitions
// that do not grow a dimension which is already over its configured limit.
func (l Limits) admits(before, after Usage) bool {
	return (l.MaxPendingMessages == 0 || after.Messages <= max(l.MaxPendingMessages, before.Messages)) &&
		(l.MaxPendingBytes == 0 || after.Bytes <= max(l.MaxPendingBytes, before.Bytes))
}

func (s *Store) Capacity() Capacity {
	s.mu.Lock()
	defer s.mu.Unlock()
	var c Capacity
	for _, e := range s.entries {
		if e.state == nil {
			continue
		}
		c.Sessions++
		for _, m := range e.state.Queue {
			c.Queued.add(m)
			if !m.QueuedAt.IsZero() && (c.OldestQueuedAt == nil || m.QueuedAt.Before(*c.OldestQueuedAt)) {
				t := m.QueuedAt
				c.OldestQueuedAt = &t
			}
		}
		for _, m := range e.state.Inflight {
			c.Outbound.add(m.Message)
		}
		for _, m := range e.state.Inbound {
			c.InboundQoS2.add(m)
		}
	}
	return c
}
