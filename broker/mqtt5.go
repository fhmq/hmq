package broker

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"time"
	"unicode/utf8"

	mqtt5 "github.com/eclipse/paho.golang/packets"
	"github.com/fhmq/hmq/v4/broker/packets"
	"github.com/google/uuid"
)

type mqtt5State struct {
	connect        *mqtt5.Connect
	maximumPacket  uint32
	receiveMaximum uint16
	aliases        map[uint16]string // Read-loop owned; reset for every connection.
	assignedID     string
	// Publication queue is guarded by outboundSendMu, not the socket write lock.
	queued      []*packets.PublishPacket
	queuedBytes int
	draining    bool
}

var errMQTT5MessageExpired = errors.New("MQTT 5 message expired before delivery")

type mqtt5WirePacket struct {
	wire mqtt5.Packet
}

func (p *mqtt5WirePacket) Unpack(io.Reader) error   { return errors.New("outbound-only MQTT 5 packet") }
func (p *mqtt5WirePacket) String() string           { return fmt.Sprintf("%T", p.wire) }
func (p *mqtt5WirePacket) Details() packets.Details { return packets.Details{} }

func (p *mqtt5WirePacket) Write(w io.Writer) error { _, err := p.wire.WriteTo(w); return err }

func readClientConnect(conn net.Conn) (*packets.ConnectPacket, *mqtt5State, error) {
	frame, err := readMQTTFrame(conn, mqtt5MaximumPacketSize)
	if err != nil {
		return nil, nil, err
	}
	c := mqtt5Cursor{data: frame}
	header := c.u8()
	c.vbi()
	name := c.str()
	version := c.u8()
	if header != 0x10 {
		return nil, nil, errors.New("first MQTT packet must be CONNECT")
	}
	if name != "MQTT" || version != 5 {
		p, err := packets.ReadPacket(bytes.NewReader(frame))
		if err != nil {
			return nil, nil, err
		}
		connect, ok := p.(*packets.ConnectPacket)
		if !ok {
			return nil, nil, errors.New("expected CONNECT")
		}
		return connect, nil, nil
	}
	state := &mqtt5State{maximumPacket: 268435455, receiveMaximum: 128, aliases: make(map[uint16]string)}
	if err := validateMQTT5Frame(frame); err != nil {
		return nil, state, err
	}
	p, err := mqtt5.ReadPacket(bytes.NewReader(frame))
	if err != nil {
		return nil, state, err
	}
	msg := p.Content.(*mqtt5.Connect)
	state.connect = msg
	if msg.Properties.MaximumPacketSize != nil {
		state.maximumPacket = *msg.Properties.MaximumPacketSize
	}
	if msg.Properties.ReceiveMaximum != nil && *msg.Properties.ReceiveMaximum < state.receiveMaximum {
		state.receiveMaximum = *msg.Properties.ReceiveMaximum
	}
	if msg.Properties.AuthMethod != "" {
		return nil, state, &mqtt5Error{0x8c, "enhanced authentication is unavailable"}
	}
	if msg.Properties.AuthData != nil {
		return nil, state, &mqtt5Error{0x82, "authentication data without method"}
	}
	if msg.WillProperties != nil && msg.WillProperties.PayloadFormat != nil && *msg.WillProperties.PayloadFormat == 1 && !utf8.Valid(msg.WillMessage) {
		return nil, state, &mqtt5Error{0x99, "will payload is not UTF-8"}
	}
	if msg.ClientID == "" {
		msg.ClientID = "hmq-" + uuid.NewString()
		state.assignedID = msg.ClientID
	}
	legacy := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	legacy.ProtocolName, legacy.ProtocolVersion = "MQTT", 4
	// MQTT 5 Session Expiry is negotiated to zero, independently of Clean Start.
	legacy.CleanSession = true
	legacy.ClientIdentifier, legacy.Keepalive = msg.ClientID, msg.KeepAlive
	legacy.Username, legacy.Password, legacy.UsernameFlag, legacy.PasswordFlag = msg.Username, msg.Password, msg.UsernameFlag, msg.PasswordFlag
	legacy.WillFlag, legacy.WillQos, legacy.WillRetain = msg.WillFlag, msg.WillQOS, msg.WillRetain
	legacy.WillTopic, legacy.WillMessage = msg.WillTopic, msg.WillMessage
	return legacy, state, nil
}

func writeMQTT5(w io.Writer, state *mqtt5State, p mqtt5.Packet) error {
	var bodySize uint64
	for _, part := range p.Buffers() {
		bodySize += uint64(len(part))
	}
	wireSize := bodySize + 2
	for n := bodySize; n >= 128; n /= 128 {
		wireSize++
	}
	if bodySize > 268435455 || wireSize > uint64(state.maximumPacket) {
		return &mqtt5Error{0x95, "outgoing packet exceeds peer maximum"}
	}
	var data bytes.Buffer
	data.Grow(int(wireSize))
	if _, err := p.WriteTo(&data); err != nil {
		return err
	}
	if uint64(data.Len()) > uint64(state.maximumPacket) {
		return &mqtt5Error{0x95, "outgoing packet exceeds peer maximum"}
	}
	n, err := w.Write(data.Bytes())
	if err == nil && n != data.Len() {
		err = io.ErrShortWrite
	}
	return err
}

func (b *Broker) writeConnack(conn net.Conn, state *mqtt5State, packet *packets.ConnackPacket) error {
	if state == nil {
		return packet.Write(conn)
	}
	reason := byte(0)
	if packet.ReturnCode != 0 {
		reason = map[byte]byte{1: 0x84, 2: 0x85, 3: 0x88, 4: 0x86, 5: 0x87}[packet.ReturnCode]
		if reason == 0 {
			reason = 0x80
		}
	}
	return b.writeMQTT5Connack(conn, state, reason)
}

func (b *Broker) writeMQTT5Connack(conn net.Conn, state *mqtt5State, reason byte) error {
	zero := uint32(0)
	maximum := uint32(mqtt5MaximumPacketSize)
	aliases := uint16(mqtt5TopicAliasMaximum)
	props := &mqtt5.Properties{}
	if reason == 0 {
		props.SessionExpiryInterval = &zero
		props.MaximumPacketSize = &maximum
		props.TopicAliasMaximum = &aliases
		props.AssignedClientID = state.assignedID
		if b.quorumEnabled() || b.sessionLog != nil {
			qos := byte(1)
			props.MaximumQOS = &qos
		}
		if b.quorumEnabled() {
			no := byte(0)
			props.RetainAvailable = &no
			props.SharedSubAvailable = &no
		}
	}
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	defer conn.SetWriteDeadline(time.Time{})
	return writeMQTT5(conn, state, &mqtt5.Connack{ReasonCode: reason, Properties: props})
}

func publicationMetadata(p *mqtt5.Properties, origin string) *packets.PublishMetadata {
	metadata := packets.CloneMetadata(&packets.PublishMetadata{Properties: p, Origin: origin})
	if metadata.Properties == nil {
		metadata.Properties = &mqtt5.Properties{}
	}
	metadata.Properties.TopicAlias = nil
	if p != nil && p.MessageExpiry != nil {
		metadata.ExpiresAt = time.Now().Add(time.Duration(*p.MessageExpiry) * time.Second)
	}
	if len(metadata.Properties.Pack(mqtt5.PUBLISH)) == 0 {
		metadata.Properties = nil
	}
	return metadata
}

func (c *client) readClientPacket(r io.Reader) (packets.ControlPacket, *mqtt5.ControlPacket, error) {
	if c.mqtt5 == nil {
		p, err := packets.ReadPacket(r)
		if publish, ok := p.(*packets.PublishPacket); ok {
			publish.Metadata = &packets.PublishMetadata{Origin: c.info.clientID}
		}
		return p, nil, err
	}
	frame, err := readMQTTFrame(r, mqtt5MaximumPacketSize)
	if err != nil {
		return nil, nil, err
	}
	if err = validateMQTT5Frame(frame); err != nil {
		return nil, nil, err
	}
	p, err := mqtt5.ReadPacket(bytes.NewReader(frame))
	if err != nil {
		return nil, nil, err
	}
	var result packets.ControlPacket
	switch v := p.Content.(type) {
	case *mqtt5.Publish:
		if alias := v.Properties.TopicAlias; alias != nil {
			if v.Topic != "" {
				c.mqtt5.aliases[*alias] = v.Topic
			} else {
				v.Topic = c.mqtt5.aliases[*alias]
			}
		}
		if !validPublishTopic(v.Topic) {
			return nil, nil, &mqtt5Error{0x90, "empty or unknown aliased topic"}
		}
		if c.broker.quorumEnabled() && (v.QoS > 1 || v.Retain) || c.broker.sessionLog != nil && v.QoS > 1 {
			return nil, nil, &mqtt5Error{0x9b, "publication exceeds negotiated capabilities"}
		}
		if v.Properties.PayloadFormat != nil && *v.Properties.PayloadFormat == 1 && !utf8.Valid(v.Payload) {
			return nil, nil, &mqtt5Error{0x99, "payload is not UTF-8"}
		}
		pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		pub.TopicName, pub.Qos, pub.Retain, pub.Dup, pub.MessageID, pub.Payload = v.Topic, v.QoS, v.Retain, v.Duplicate, v.PacketID, v.Payload
		pub.Metadata = publicationMetadata(v.Properties, c.info.clientID)
		result = pub
	case *mqtt5.Subscribe:
		sub := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
		sub.MessageID = v.PacketID
		for _, s := range v.Subscriptions {
			sub.Topics = append(sub.Topics, s.Topic)
			sub.Qoss = append(sub.Qoss, s.QoS)
		}
		result = sub
	case *mqtt5.Unsubscribe:
		sub := packets.NewControlPacket(packets.Unsubscribe).(*packets.UnsubscribePacket)
		sub.MessageID, sub.Topics = v.PacketID, v.Topics
		result = sub
	case *mqtt5.Puback:
		a := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		a.MessageID = v.PacketID
		result = a
	case *mqtt5.Pubrec:
		a := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
		a.MessageID = v.PacketID
		result = a
	case *mqtt5.Pubrel:
		a := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
		a.MessageID = v.PacketID
		result = a
	case *mqtt5.Pubcomp:
		a := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
		a.MessageID = v.PacketID
		result = a
	case *mqtt5.Pingreq:
		result = packets.NewControlPacket(packets.Pingreq)
	case *mqtt5.Disconnect:
		if v.Properties.SessionExpiryInterval != nil && *v.Properties.SessionExpiryInterval != 0 {
			return nil, nil, &mqtt5Error{0x82, "cannot increase zero session expiry"}
		}
		result = packets.NewControlPacket(packets.Disconnect)
	default:
		return nil, nil, &mqtt5Error{0x82, "unexpected MQTT 5 packet"}
	}
	return result, p, nil
}

func (c *client) writeClientPacket(w io.Writer, p packets.ControlPacket) error {
	if wire, ok := p.(*mqtt5WirePacket); ok {
		return writeMQTT5(w, c.mqtt5, wire.wire)
	}
	if c.mqtt5 == nil {
		return p.Write(w)
	}
	var wire mqtt5.Packet
	switch v := p.(type) {
	case *packets.PublishPacket:
		props := &mqtt5.Properties{}
		if v.Metadata != nil {
			m := packets.CloneMetadata(v.Metadata)
			if m.Properties != nil {
				props = m.Properties
			}
			if !m.ExpiresAt.IsZero() {
				remaining := time.Until(m.ExpiresAt)
				if remaining <= 0 {
					return errMQTT5MessageExpired
				}
				seconds := uint32((remaining + time.Second - 1) / time.Second)
				props.MessageExpiry = &seconds
			}
		}
		wire = &mqtt5.Publish{Topic: v.TopicName, Payload: v.Payload, PacketID: v.MessageID, QoS: v.Qos, Retain: v.Retain, Duplicate: v.Dup, Properties: props}
	case *packets.PubackPacket:
		wire = &mqtt5.Puback{PacketID: v.MessageID, Properties: &mqtt5.Properties{}}
	case *packets.PubrecPacket:
		wire = &mqtt5.Pubrec{PacketID: v.MessageID, Properties: &mqtt5.Properties{}}
	case *packets.PubrelPacket:
		wire = &mqtt5.Pubrel{PacketID: v.MessageID, Properties: &mqtt5.Properties{}}
	case *packets.PubcompPacket:
		wire = &mqtt5.Pubcomp{PacketID: v.MessageID, Properties: &mqtt5.Properties{}}
	case *packets.SubackPacket:
		wire = &mqtt5.Suback{PacketID: v.MessageID, Reasons: v.ReturnCodes, Properties: &mqtt5.Properties{}}
	case *packets.PingrespPacket:
		wire = &mqtt5.Pingresp{}
	case *packets.DisconnectPacket:
		wire = &mqtt5.Disconnect{Properties: &mqtt5.Properties{}}
	default:
		return fmt.Errorf("unsupported MQTT 5 output %T", p)
	}
	return writeMQTT5(w, c.mqtt5, wire)
}

func (c *client) mqtt5Disconnect(reason byte) {
	if c.mqtt5 != nil {
		_ = c.WriterPacket(&mqtt5WirePacket{wire: &mqtt5.Disconnect{ReasonCode: reason, Properties: &mqtt5.Properties{}}})
	}
}

// Negative PUBREC ends a QoS2 exchange; it must not generate PUBREL.
func (c *client) processMQTT5NegativeAck(p *mqtt5.ControlPacket) bool {
	v, ok := p.Content.(*mqtt5.Pubrec)
	if !ok || v.ReasonCode < 0x80 {
		return false
	}
	c.outboundSendMu.Lock()
	c.inflightMu.Lock()
	if entry, ok := c.inflight[v.PacketID]; ok && entry.packet.Qos == 2 && entry.status == Publish {
		delete(c.inflight, v.PacketID)
		if c.session != nil {
			c.session.UpdateOutbound(c.outboundOwner, v.PacketID, nil)
		}
	}
	c.inflightMu.Unlock()
	c.outboundSendMu.Unlock()
	c.pumpMQTT5Queue()
	return true
}

func publicationSize(p *packets.PublishPacket) int {
	size := len(p.TopicName) + len(p.Payload) + 64
	if p.Metadata != nil && p.Metadata.Properties != nil {
		size += len(p.Metadata.Properties.Pack(mqtt5.PUBLISH))
	}
	return size
}

func (c *client) pumpMQTT5Queue() {
	if c.mqtt5 == nil {
		return
	}
	c.outboundSendMu.Lock()
	if c.mqtt5.draining {
		c.outboundSendMu.Unlock()
		return
	}
	c.mqtt5.draining = true
	c.outboundSendMu.Unlock()
	for {
		c.outboundSendMu.Lock()
		c.inflightMu.RLock()
		full := len(c.inflight) >= int(c.mqtt5.receiveMaximum)
		c.inflightMu.RUnlock()
		if full || len(c.mqtt5.queued) == 0 {
			c.mqtt5.draining = false
			c.outboundSendMu.Unlock()
			return
		}
		p := c.mqtt5.queued[0]
		c.mqtt5.queued[0] = nil
		c.mqtt5.queued = c.mqtt5.queued[1:]
		c.mqtt5.queuedBytes -= publicationSize(p)
		c.outboundSendMu.Unlock()
		if p.Metadata != nil && !p.Metadata.ExpiresAt.IsZero() && !time.Now().Before(p.Metadata.ExpiresAt) {
			continue
		}
		if err := c.sendPublication(p, false, true); err != nil {
			c.outboundSendMu.Lock()
			c.mqtt5.draining = false
			c.outboundSendMu.Unlock()
			// Wake the reader; it submits close after releasing route ownership.
			_ = c.transport.Close()
			return
		}
	}
}
