package broker

import (
	"errors"
	"net"
	"time"

	mqtt5 "github.com/eclipse/paho.golang/packets"
	"github.com/fhmq/hmq/v4/broker/packets"
	"github.com/fhmq/hmq/v4/broker/sessionstore"
)

func (c *client) writeQoS2Completion(id uint16, found bool) error {
	if c.mqtt5 != nil && !found {
		return c.WriterPacket(&mqtt5WirePacket{wire: &mqtt5.Pubcomp{PacketID: id, ReasonCode: 0x92, Properties: &mqtt5.Properties{}}})
	}
	ack := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
	ack.MessageID = id
	return c.WriterPacket(ack)
}

func (c *client) receiveDurableQoS2(p *packets.PublishPacket) error {
	if c.broker.bridgeMQ != nil {
		return errors.New("durable QoS2 does not support bridge routing")
	}
	unlock := c.broker.lockClientConnect(c.info.clientID)
	defer unlock()
	if owner, _ := c.broker.clients.Load(c.info.clientID); owner != c {
		return net.ErrClosed
	}
	err := c.broker.sessionLog.Receive(c.info.clientID, c.durableEpoch, p.MessageID, sessionstore.Message{
		Topic: p.TopicName, Payload: p.Payload, QoS: p.Qos, Retain: p.Retain, Metadata: p.Metadata,
	}, c.maxAwaitingRel)
	if err != nil {
		return err
	}
	ack := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
	ack.MessageID = p.MessageID
	return c.WriterPacket(ack)
}

func (c *client) releaseDurableQoS2(id uint16) error {
	b := c.broker
	unlock := b.lockClientConnect(c.info.clientID)
	if owner, _ := b.clients.Load(c.info.clientID); owner != c {
		unlock()
		return net.ErrClosed
	}
	message, found, err := b.sessionLog.Release(c.info.clientID, c.durableEpoch, id, time.Now())
	unlock()
	if err != nil {
		return err
	}
	if b.quorumEnabled() {
		return c.writeQoS2Completion(id, found)
	}
	if message.Topic != "" {
		p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		p.TopicName, p.Payload, p.Qos, p.Retain, p.Metadata = message.Topic, message.Payload, message.QoS, message.Retain, message.Metadata
		if p.Retain {
			if err := b.topicsMgr.Retain(p); err != nil {
				return err
			}
		}
		if err := c.forwardPublishMessage(p); err != nil {
			return err
		}
		if err := b.publishTransientLocal(p, true); err != nil {
			return err
		}
		b.pumpQoS2Subscribers(false)
	}
	return c.writeQoS2Completion(id, found)
}

func (b *Broker) pumpQoS2Subscribers(fence bool) {
	b.clients.Range(func(_, value any) bool {
		sub := value.(*client)
		if sub.durable() {
			if fence {
				epoch, err := b.quorumRuntime.sessions.Store.Epoch(sub.info.clientID)
				if err != nil || epoch != sub.durableEpoch {
					sub.Close()
					return true
				}
			}
			if err := sub.pumpSessionQueue(); err != nil {
				sub.Close()
			}
		}
		return true
	})
}

func (b *Broker) pumpQuorumSessions() {
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-t.C:
			b.pumpQoS2Subscribers(true)
		}
	}
}

func (c *client) deleteDurableSession() {
	b := c.broker
	unlock := b.lockClientConnect(c.info.clientID)
	defer unlock()
	if owner, _ := b.clients.Load(c.info.clientID); owner != c {
		return
	}
	state, version, err := b.sessionLog.Get(c.info.clientID)
	if err == nil && state != nil && state.Epoch == c.durableEpoch {
		if _, err := b.sessionLog.Put(c.info.clientID, version, nil); err == nil {
			b.deleteSessionIndex(c.info.clientID)
			b.sessionMgr.Del(c.info.clientID)
		}
	}
}
