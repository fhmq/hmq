package broker

import (
	"errors"
	"time"

	mqtt5 "github.com/eclipse/paho.golang/packets"
	"github.com/fhmq/hmq/v4/broker/packets"
	"github.com/fhmq/hmq/v4/broker/replication"
	"github.com/fhmq/hmq/v4/broker/sessionstore"
)

type brokerCapacity struct {
	Raft                      *replication.Capacity  `json:"raft,omitempty"`
	PersistentSessions        *sessionstore.Capacity `json:"persistentSessions,omitempty"`
	SessionQuota              sessionstore.Limits    `json:"sessionQuota"`
	OldestQueuedAgeSeconds    *float64               `json:"oldestQueuedAgeSeconds,omitempty"`
	ConnectedOutboundInflight int                    `json:"connectedOutboundInflight"`
	SlowConsumerDisconnects   uint64                 `json:"slowConsumerDisconnects"`
	QuotaRejections           uint64                 `json:"quotaRejections"`
}

func (b *Broker) capacity() brokerCapacity {
	out := brokerCapacity{
		SlowConsumerDisconnects: b.slowConsumerDisconnects.Load(),
		QuotaRejections:         b.sessionQuotaRejections.Load(),
	}
	if b.config != nil {
		out.SessionQuota = b.config.SessionQuota
	}
	if b.quorumRuntime != nil {
		r := b.quorumRuntime.node.Capacity()
		out.Raft = &r
	}
	if backend, ok := b.sessionLog.(interface{ Capacity() sessionstore.Capacity }); ok {
		c := backend.Capacity()
		out.PersistentSessions = &c
		if c.OldestQueuedAt != nil {
			age := max(0, time.Since(*c.OldestQueuedAt).Seconds())
			out.OldestQueuedAgeSeconds = &age
		}
	}
	b.clients.Range(func(_, v any) bool {
		if c, ok := v.(*client); ok {
			c.inflightMu.RLock()
			out.ConnectedOutboundInflight += len(c.inflight)
			c.inflightMu.RUnlock()
		}
		return true
	})
	return out
}

func (b *Broker) recordSlowConsumer(c *client) {
	if c.slowConsumer.CompareAndSwap(false, true) {
		b.slowConsumerDisconnects.Add(1)
	}
}

// MQTT 5 can reject PUBLISH without a successful acknowledgement. On PUBREL,
// no quota reason is defined for PUBCOMP: disconnect and retain inbound state.
func (c *client) rejectQuota(p *packets.PublishPacket, err error) bool {
	if !errors.Is(err, sessionstore.ErrQuota) {
		return false
	}
	c.broker.sessionQuotaRejections.Add(1)
	if c.mqtt5 == nil || p == nil || p.Qos == 0 {
		c.mqtt5Disconnect(0x97)
		c.Close()
		return true
	}
	var ack mqtt5.Packet
	if p.Qos == 1 {
		ack = &mqtt5.Puback{PacketID: p.MessageID, ReasonCode: 0x97, Properties: &mqtt5.Properties{}}
	} else {
		ack = &mqtt5.Pubrec{PacketID: p.MessageID, ReasonCode: 0x97, Properties: &mqtt5.Properties{}}
	}
	if err := c.WriterPacket(&mqtt5WirePacket{wire: ack}); err != nil {
		c.Close()
	}
	return true
}
