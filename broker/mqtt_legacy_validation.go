package broker

import (
	"bytes"

	"github.com/fhmq/hmq/v4/broker/packets"
)

func validateLegacyConnect(frame []byte) error {
	c := mqtt5Cursor{data: frame}
	c.u8()
	c.vbi()
	c.str() // Protocol name and version are checked by the CONNECT handler.
	c.u8()
	flags := c.u8()
	c.u16()
	c.str()
	if flags&4 != 0 {
		if !validPublishTopic(c.str()) {
			c.fail("invalid will topic")
		}
		c.binary()
	}
	if flags&128 != 0 {
		c.str()
	}
	if flags&64 != 0 {
		c.binary()
	}
	if c.pos != len(frame) {
		c.fail("unexpected CONNECT data")
	}
	return c.err
}

// Admit only client-originated packets with complete fields before handing the
// frame to the legacy decoder. PUBLISH can reuse the owned frame as its payload.
func decodeLegacyClientPacket(frame []byte) (packets.ControlPacket, error) {
	c := mqtt5Cursor{data: frame}
	header := c.u8()
	remaining := c.vbi()
	kind, flags := header>>4, header&15
	if remaining != len(frame)-c.pos {
		c.fail("remaining length does not match frame")
	}
	if kind != packets.Publish {
		var expected byte
		if kind == packets.Pubrel || kind == packets.Subscribe || kind == packets.Unsubscribe {
			expected = 2
		}
		if flags != expected {
			c.fail("invalid fixed header flags")
		}
	}
	identifier := func() uint16 {
		id := c.u16()
		if id == 0 {
			c.fail("packet identifier is zero")
		}
		return id
	}
	switch kind {
	case packets.Publish:
		qos := flags >> 1 & 3
		if qos == 3 || qos == 0 && flags&8 != 0 {
			c.fail("invalid PUBLISH flags")
		}
		topic := c.str()
		if !validPublishTopic(topic) {
			c.fail("invalid PUBLISH topic")
		}
		var id uint16
		if qos != 0 {
			id = identifier()
		}
		if c.err != nil {
			return nil, c.err
		}
		return &packets.PublishPacket{
			FixedHeader: packets.FixedHeader{MessageType: kind, Qos: qos, Dup: flags&8 != 0, Retain: flags&1 != 0, RemainingLength: remaining},
			TopicName:   topic, MessageID: id, Payload: frame[c.pos:],
		}, nil
	case packets.Puback, packets.Pubrec, packets.Pubrel, packets.Pubcomp:
		identifier()
	case packets.Subscribe, packets.Unsubscribe:
		identifier()
		if c.pos == len(frame) {
			c.fail("empty subscription packet")
		}
		for c.pos < len(frame) && c.err == nil {
			if !validSubscribeTopic(c.str()) {
				c.fail("invalid topic filter")
			}
			if kind == packets.Subscribe && c.u8() > 2 {
				c.fail("invalid subscription QoS")
			}
		}
	case packets.Pingreq, packets.Disconnect:
	default:
		c.fail("unexpected client packet")
	}
	if c.pos != len(frame) {
		c.fail("unexpected packet data")
	}
	if c.err != nil {
		return nil, c.err
	}
	return packets.ReadPacket(bytes.NewReader(frame))
}
