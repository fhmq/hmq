package broker

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	mqtt5 "github.com/eclipse/paho.golang/packets"
	"github.com/fhmq/hmq/v4/broker/packets"
)

func mqtt5ITBroker(t *testing.T, config *Config) string {
	t.Helper()
	if config == nil {
		config = &Config{}
	}
	config.Host, config.Port = "127.0.0.1", "0"
	b, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	b.Start()
	t.Cleanup(b.Close)
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		b.lifecycleMu.Lock()
		var addr string
		for listener := range b.listeners {
			addr = listener.Addr().String()
		}
		b.lifecycleMu.Unlock()
		if addr != "" {
			return addr
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("broker listener did not start")
	return ""
}

func mqtt5ITDial(t *testing.T, addr string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func mqtt5ITWrite(t *testing.T, conn net.Conn, packet mqtt5.Packet) {
	t.Helper()
	if err := conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := packet.WriteTo(conn); err != nil {
		t.Fatalf("write %T: %v", packet, err)
	}
}

func mqtt5ITRead[T mqtt5.Packet](t *testing.T, conn net.Conn) T {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	packet, err := mqtt5.ReadPacket(conn)
	if err != nil {
		t.Fatalf("read MQTT 5 packet: %v", err)
	}
	result, ok := packet.Content.(T)
	if !ok {
		t.Fatalf("got %T (%v), want %T", packet.Content, packet, *new(T))
	}
	return result
}

func mqtt5ITQuiet(t *testing.T, conn net.Conn) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	var first [1]byte
	n, err := conn.Read(first[:])
	if n != 0 {
		t.Fatalf("unexpected packet byte %02x", first[0])
	}
	timeout, ok := err.(net.Error)
	if !ok || !timeout.Timeout() {
		t.Fatalf("expected live, quiet connection; got %v", err)
	}
}

func mqtt5ITPing(t *testing.T, conn net.Conn) {
	t.Helper()
	mqtt5ITWrite(t, conn, &mqtt5.Pingreq{})
	mqtt5ITRead[*mqtt5.Pingresp](t, conn)
}

func mqtt5ITConnect(t *testing.T, addr, id string, configure func(*mqtt5.Connect)) (net.Conn, *mqtt5.Connack) {
	t.Helper()
	conn := mqtt5ITDial(t, addr)
	connect := &mqtt5.Connect{ProtocolName: "MQTT", ProtocolVersion: 5, ClientID: id, CleanStart: true, Properties: &mqtt5.Properties{}}
	if configure != nil {
		configure(connect)
	}
	mqtt5ITWrite(t, conn, connect)
	return conn, mqtt5ITRead[*mqtt5.Connack](t, conn)
}

func mqtt5ITClient(t *testing.T, addr, id string, configure func(*mqtt5.Connect)) net.Conn {
	t.Helper()
	conn, ack := mqtt5ITConnect(t, addr, id, configure)
	if ack.ReasonCode != 0 {
		t.Fatalf("CONNECT rejected: %#v", ack)
	}
	return conn
}

func mqtt5ITSubscribe(t *testing.T, conn net.Conn, id uint16, option mqtt5.SubOptions, props *mqtt5.Properties) {
	t.Helper()
	if props == nil {
		props = &mqtt5.Properties{}
	}
	mqtt5ITWrite(t, conn, &mqtt5.Subscribe{PacketID: id, Subscriptions: []mqtt5.SubOptions{option}, Properties: props})
	ack := mqtt5ITRead[*mqtt5.Suback](t, conn)
	if ack.PacketID != id || !bytes.Equal(ack.Reasons, []byte{option.QoS}) {
		t.Fatalf("SUBACK: %#v", ack)
	}
}

func mqtt5ITPublish(t *testing.T, conn net.Conn, p *mqtt5.Publish) {
	t.Helper()
	if p.Properties == nil {
		p.Properties = &mqtt5.Properties{}
	}
	mqtt5ITWrite(t, conn, p)
	switch p.QoS {
	case 1:
		ack := mqtt5ITRead[*mqtt5.Puback](t, conn)
		if ack.PacketID != p.PacketID || ack.ReasonCode >= 0x80 {
			t.Fatalf("PUBACK: %#v", ack)
		}
	case 2:
		rec := mqtt5ITRead[*mqtt5.Pubrec](t, conn)
		if rec.PacketID != p.PacketID || rec.ReasonCode >= 0x80 {
			t.Fatalf("PUBREC: %#v", rec)
		}
		mqtt5ITWrite(t, conn, &mqtt5.Pubrel{PacketID: p.PacketID, Properties: &mqtt5.Properties{}})
		comp := mqtt5ITRead[*mqtt5.Pubcomp](t, conn)
		if comp.PacketID != p.PacketID || comp.ReasonCode != 0 {
			t.Fatalf("PUBCOMP: %#v", comp)
		}
	}
}

func mqtt5ITAck(t *testing.T, conn net.Conn, p *mqtt5.Publish) {
	t.Helper()
	if p.QoS == 1 {
		mqtt5ITWrite(t, conn, &mqtt5.Puback{PacketID: p.PacketID, Properties: &mqtt5.Properties{}})
	} else if p.QoS == 2 {
		mqtt5ITWrite(t, conn, &mqtt5.Pubrec{PacketID: p.PacketID, Properties: &mqtt5.Properties{}})
		rel := mqtt5ITRead[*mqtt5.Pubrel](t, conn)
		if rel.PacketID != p.PacketID || rel.ReasonCode != 0 {
			t.Fatalf("PUBREL: %#v", rel)
		}
		mqtt5ITWrite(t, conn, &mqtt5.Pubcomp{PacketID: p.PacketID, Properties: &mqtt5.Properties{}})
	}
}

func TestMQTT5IntegrationConnectCapabilities(t *testing.T) {
	addr := mqtt5ITBroker(t, nil)
	expiry := uint32(3600)
	conn, ack := mqtt5ITConnect(t, addr, "", func(c *mqtt5.Connect) {
		c.CleanStart = false
		c.Properties.SessionExpiryInterval = &expiry
	})
	if ack.ReasonCode != 0 || ack.SessionPresent || ack.Properties == nil {
		t.Fatalf("CONNACK: %#v", ack)
	}
	p := ack.Properties
	if p.SessionExpiryInterval == nil || *p.SessionExpiryInterval != 0 {
		t.Fatalf("session expiry not negotiated to zero: %#v", p)
	}
	if p.AssignedClientID == "" || p.MaximumPacketSize == nil || *p.MaximumPacketSize == 0 || p.TopicAliasMaximum == nil || *p.TopicAliasMaximum == 0 {
		t.Fatalf("missing assigned ID or capabilities: %#v", p)
	}
	if p.MaximumQOS != nil && *p.MaximumQOS != 2 {
		t.Fatalf("unexpected QoS limit: %d", *p.MaximumQOS)
	}
	mqtt5ITPing(t, conn)
	mqtt5ITWrite(t, conn, &mqtt5.Disconnect{Properties: &mqtt5.Properties{}})
	// Reading EOF ensures the old session has closed before reconnecting.
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var one [1]byte
	if n, err := conn.Read(one[:]); n != 0 || err != io.EOF {
		t.Fatalf("disconnect: n=%d err=%v", n, err)
	}
	_, second := mqtt5ITConnect(t, addr, p.AssignedClientID, func(c *mqtt5.Connect) { c.CleanStart = false })
	if second.ReasonCode != 0 || second.SessionPresent {
		t.Fatalf("zero-expiry reconnect: %#v", second)
	}
}

type mqtt5ITAuth struct{}

func (mqtt5ITAuth) CheckConnect(_, username, password string) bool {
	return username == "alice" && password == "secret" ||
		username == "" && password == "password-only-secret"
}
func (mqtt5ITAuth) CheckACL(_, _, _, _, _ string) bool { return true }

func TestMQTT5IntegrationAuthentication(t *testing.T) {
	addr := mqtt5ITBroker(t, &Config{Plugin: Plugins{Auth: mqtt5ITAuth{}}})
	for _, tc := range []struct {
		name, username, password, method string
		reason                           byte
	}{
		{"password accepted", "alice", "secret", "", 0},
		{"password refused", "alice", "wrong", "", 0x87},
		{"password only accepted", "", "password-only-secret", "", 0},
		{"password only refused", "", "wrong", "", 0x87},
		{"enhanced auth unsupported", "alice", "secret", "unsupported", 0x8c},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn, ack := mqtt5ITConnect(t, addr, tc.name, func(c *mqtt5.Connect) {
				c.UsernameFlag, c.PasswordFlag = tc.username != "", true
				c.Username, c.Password, c.Properties.AuthMethod = tc.username, []byte(tc.password), tc.method
			})
			if ack.ReasonCode != tc.reason {
				t.Fatalf("CONNACK reason=%02x, want %02x", ack.ReasonCode, tc.reason)
			}
			if tc.reason == 0 {
				mqtt5ITPing(t, conn)
			}
		})
	}
}

func TestMQTT5IntegrationQoSRoundTrip(t *testing.T) {
	for qos := byte(0); qos <= 2; qos++ {
		t.Run(fmt.Sprint(qos), func(t *testing.T) {
			addr := mqtt5ITBroker(t, nil)
			sub := mqtt5ITClient(t, addr, "subscriber", nil)
			pub := mqtt5ITClient(t, addr, "publisher", nil)
			mqtt5ITSubscribe(t, sub, 1, mqtt5.SubOptions{Topic: "roundtrip", QoS: qos}, nil)
			mqtt5ITPublish(t, pub, &mqtt5.Publish{Topic: "roundtrip", Payload: []byte{0, 1, 255}, PacketID: 9, QoS: qos})
			got := mqtt5ITRead[*mqtt5.Publish](t, sub)
			if got.Topic != "roundtrip" || got.QoS != qos || !bytes.Equal(got.Payload, []byte{0, 1, 255}) || qos > 0 && got.PacketID == 0 {
				t.Fatalf("delivery: %#v", got)
			}
			mqtt5ITAck(t, sub, got)
			mqtt5ITPing(t, sub)
		})
	}
}

func mqtt5ITLegacyWrite(t *testing.T, conn net.Conn, p packets.ControlPacket) {
	t.Helper()
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if err := p.Write(conn); err != nil {
		t.Fatal(err)
	}
}

func mqtt5ITLegacyRead(t *testing.T, conn net.Conn) packets.ControlPacket {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	p, err := packets.ReadPacket(conn)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestMQTT5IntegrationMixedVersions(t *testing.T) {
	addr := mqtt5ITBroker(t, nil)
	v5 := mqtt5ITClient(t, addr, "v5", nil)
	v4 := mqtt5ITDial(t, addr)
	connect := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	connect.ProtocolName, connect.ProtocolVersion, connect.CleanSession, connect.ClientIdentifier = "MQTT", 4, true, "v4"
	mqtt5ITLegacyWrite(t, v4, connect)
	if ack, ok := mqtt5ITLegacyRead(t, v4).(*packets.ConnackPacket); !ok || ack.ReturnCode != 0 {
		t.Fatalf("legacy CONNACK: %#v", ack)
	}
	subscribe := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	subscribe.MessageID, subscribe.Topics, subscribe.Qoss = 1, []string{"mixed/to4"}, []byte{1}
	mqtt5ITLegacyWrite(t, v4, subscribe)
	if ack, ok := mqtt5ITLegacyRead(t, v4).(*packets.SubackPacket); !ok || !bytes.Equal(ack.ReturnCodes, []byte{1}) {
		t.Fatalf("legacy SUBACK: %#v", ack)
	}
	mqtt5ITSubscribe(t, v5, 1, mqtt5.SubOptions{Topic: "mixed/to5", QoS: 1}, nil)
	mqtt5ITPublish(t, v5, &mqtt5.Publish{Topic: "mixed/to4", Payload: []byte("five"), QoS: 1, PacketID: 10, Properties: &mqtt5.Properties{ContentType: "text/plain"}})
	got4, ok := mqtt5ITLegacyRead(t, v4).(*packets.PublishPacket)
	if !ok || got4.TopicName != "mixed/to4" || string(got4.Payload) != "five" || got4.Qos != 1 {
		t.Fatalf("5 to 4: %#v", got4)
	}
	ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
	ack.MessageID = got4.MessageID
	mqtt5ITLegacyWrite(t, v4, ack)
	publish := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	publish.TopicName, publish.Payload, publish.Qos, publish.MessageID = "mixed/to5", []byte("four"), 1, 12
	mqtt5ITLegacyWrite(t, v4, publish)
	if ack, ok := mqtt5ITLegacyRead(t, v4).(*packets.PubackPacket); !ok || ack.MessageID != 12 {
		t.Fatalf("legacy PUBACK: %#v", ack)
	}
	got5 := mqtt5ITRead[*mqtt5.Publish](t, v5)
	if got5.Topic != "mixed/to5" || string(got5.Payload) != "four" || got5.QoS != 1 {
		t.Fatalf("4 to 5: %#v", got5)
	}
	mqtt5ITAck(t, v5, got5)
	mqtt5ITPing(t, v5)
}

func TestMQTT5IntegrationProperties(t *testing.T) {
	addr := mqtt5ITBroker(t, nil)
	sub := mqtt5ITClient(t, addr, "subscriber", nil)
	pub := mqtt5ITClient(t, addr, "publisher", nil)
	identifier := 123
	mqtt5ITSubscribe(t, sub, 1, mqtt5.SubOptions{Topic: "properties", QoS: 1}, &mqtt5.Properties{SubscriptionIdentifier: &identifier})
	props := &mqtt5.Properties{ContentType: "application/octet-stream", ResponseTopic: "response/topic", CorrelationData: []byte{0, 255, 1}, User: []mqtt5.User{{Key: "key", Value: "first"}, {Key: "key", Value: "second"}}}
	mqtt5ITPublish(t, pub, &mqtt5.Publish{Topic: "properties", Payload: []byte("data"), QoS: 1, PacketID: 8, Properties: props})
	got := mqtt5ITRead[*mqtt5.Publish](t, sub)
	if got.Properties == nil {
		t.Fatal("missing properties")
	}
	expected := *props
	expected.SubscriptionIdentifier = &identifier
	if !reflect.DeepEqual(got.Properties, &expected) {
		t.Fatalf("properties got %#v, want %#v", got.Properties, &expected)
	}
	mqtt5ITAck(t, sub, got)
}

func TestMQTT5IntegrationSubscriptionOptions(t *testing.T) {
	addr := mqtt5ITBroker(t, nil)
	pub := mqtt5ITClient(t, addr, "publisher", nil)
	local := mqtt5ITClient(t, addr, "local", nil)
	mqtt5ITSubscribe(t, local, 1, mqtt5.SubOptions{Topic: "options", QoS: 1, NoLocal: true}, nil)
	mqtt5ITPublish(t, local, &mqtt5.Publish{Topic: "options", Payload: []byte("self"), QoS: 1, PacketID: 10})
	mqtt5ITPing(t, local)
	mqtt5ITQuiet(t, local)
	rap := mqtt5ITClient(t, addr, "rap", nil)
	mqtt5ITSubscribe(t, rap, 1, mqtt5.SubOptions{Topic: "options", QoS: 1, RetainAsPublished: true}, nil)
	mqtt5ITPublish(t, pub, &mqtt5.Publish{Topic: "options", Payload: []byte("retained"), QoS: 1, PacketID: 11, Retain: true})
	for _, tc := range []struct {
		conn   net.Conn
		retain bool
	}{{local, false}, {rap, true}} {
		got := mqtt5ITRead[*mqtt5.Publish](t, tc.conn)
		if string(got.Payload) != "retained" || got.Retain != tc.retain {
			t.Fatalf("live retain handling: %#v", got)
		}
		mqtt5ITAck(t, tc.conn, got)
	}
	for _, handling := range []byte{0, 1, 2} {
		t.Run(fmt.Sprintf("retain_handling_%d", handling), func(t *testing.T) {
			conn := mqtt5ITClient(t, addr, fmt.Sprintf("rh-%d", handling), nil)
			option := mqtt5.SubOptions{Topic: "options", QoS: 1, RetainHandling: handling}
			mqtt5ITSubscribe(t, conn, 1, option, nil)
			if handling != 2 {
				got := mqtt5ITRead[*mqtt5.Publish](t, conn)
				if !got.Retain || string(got.Payload) != "retained" {
					t.Fatalf("retained replay: %#v", got)
				}
				mqtt5ITAck(t, conn, got)
			} else {
				mqtt5ITQuiet(t, conn)
			}
			mqtt5ITSubscribe(t, conn, 2, option, nil)
			if handling == 0 {
				got := mqtt5ITRead[*mqtt5.Publish](t, conn)
				if !got.Retain || string(got.Payload) != "retained" {
					t.Fatalf("retained resubscribe: %#v", got)
				}
				mqtt5ITAck(t, conn, got)
			} else {
				mqtt5ITQuiet(t, conn)
			}
			mqtt5ITPing(t, conn)
		})
	}
}

func TestMQTT5IntegrationTopicAlias(t *testing.T) {
	addr := mqtt5ITBroker(t, nil)
	sub := mqtt5ITClient(t, addr, "subscriber", nil)
	mqtt5ITSubscribe(t, sub, 1, mqtt5.SubOptions{Topic: "alias", QoS: 1}, nil)
	pub := mqtt5ITClient(t, addr, "publisher", nil)
	alias := uint16(1)
	for i, topic := range []string{"alias", ""} {
		mqtt5ITPublish(t, pub, &mqtt5.Publish{Topic: topic, Payload: []byte{byte(i)}, QoS: 1, PacketID: uint16(i + 1), Properties: &mqtt5.Properties{TopicAlias: &alias}})
		got := mqtt5ITRead[*mqtt5.Publish](t, sub)
		if got.Topic != "alias" || !bytes.Equal(got.Payload, []byte{byte(i)}) || got.Properties.TopicAlias != nil {
			t.Fatalf("alias delivery: %#v", got)
		}
		mqtt5ITAck(t, sub, got)
	}
	mqtt5ITWrite(t, pub, &mqtt5.Disconnect{Properties: &mqtt5.Properties{}})
	_ = pub.SetReadDeadline(time.Now().Add(2 * time.Second))
	var one [1]byte
	if n, err := pub.Read(one[:]); n != 0 || err != io.EOF {
		t.Fatalf("disconnect: n=%d err=%v", n, err)
	}
	pub = mqtt5ITClient(t, addr, "publisher", nil)
	mqtt5ITWrite(t, pub, &mqtt5.Publish{Payload: []byte("invalid"), QoS: 1, PacketID: 1, Properties: &mqtt5.Properties{TopicAlias: &alias}})
	rejected := mqtt5ITRead[*mqtt5.Disconnect](t, pub)
	if rejected.ReasonCode < 0x80 {
		t.Fatalf("alias survived reconnect: %#v", rejected)
	}
	mqtt5ITQuiet(t, sub)
}

func TestMQTT5IntegrationReceiveMaximum(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		t.Run(fmt.Sprint(qos), func(t *testing.T) {
			addr := mqtt5ITBroker(t, nil)
			maximum := uint16(1)
			sub := mqtt5ITClient(t, addr, "subscriber", func(c *mqtt5.Connect) { c.Properties.ReceiveMaximum = &maximum })
			pub := mqtt5ITClient(t, addr, "publisher", nil)
			mqtt5ITSubscribe(t, sub, 1, mqtt5.SubOptions{Topic: "quota", QoS: qos}, nil)
			for i := 0; i < 3; i++ {
				mqtt5ITPublish(t, pub, &mqtt5.Publish{Topic: "quota", Payload: []byte{byte(i)}, QoS: qos, PacketID: uint16(i + 1)})
			}
			for i := 0; i < 3; i++ {
				got := mqtt5ITRead[*mqtt5.Publish](t, sub)
				if got.QoS != qos || !bytes.Equal(got.Payload, []byte{byte(i)}) {
					t.Fatalf("FIFO delivery %d: %#v", i, got)
				}
				mqtt5ITQuiet(t, sub)
				if qos == 1 {
					mqtt5ITAck(t, sub, got)
				} else {
					mqtt5ITWrite(t, sub, &mqtt5.Pubrec{PacketID: got.PacketID, Properties: &mqtt5.Properties{}})
					rel := mqtt5ITRead[*mqtt5.Pubrel](t, sub)
					if rel.PacketID != got.PacketID || rel.ReasonCode != 0 {
						t.Fatalf("PUBREL: %#v", rel)
					}
					// The receive slot must remain occupied through the PUBCOMP boundary.
					mqtt5ITQuiet(t, sub)
					mqtt5ITWrite(t, sub, &mqtt5.Pubcomp{PacketID: got.PacketID, Properties: &mqtt5.Properties{}})
				}
			}
			mqtt5ITPing(t, sub)
		})
	}
}

func TestMQTT5IntegrationNegativePubrec(t *testing.T) {
	addr := mqtt5ITBroker(t, nil)
	maximum := uint16(1)
	sub := mqtt5ITClient(t, addr, "subscriber", func(c *mqtt5.Connect) { c.Properties.ReceiveMaximum = &maximum })
	pub := mqtt5ITClient(t, addr, "publisher", nil)
	mqtt5ITSubscribe(t, sub, 1, mqtt5.SubOptions{Topic: "negative", QoS: 2}, nil)
	for i := 0; i < 2; i++ {
		mqtt5ITPublish(t, pub, &mqtt5.Publish{Topic: "negative", Payload: []byte{byte(i)}, QoS: 2, PacketID: uint16(i + 1)})
	}
	first := mqtt5ITRead[*mqtt5.Publish](t, sub)
	if !bytes.Equal(first.Payload, []byte{0}) {
		t.Fatalf("first: %#v", first)
	}
	mqtt5ITQuiet(t, sub)
	mqtt5ITWrite(t, sub, &mqtt5.Pubrec{PacketID: first.PacketID, ReasonCode: 0x80, Properties: &mqtt5.Properties{}})
	// The very next packet must be the queued publication, never PUBREL.
	second := mqtt5ITRead[*mqtt5.Publish](t, sub)
	if !bytes.Equal(second.Payload, []byte{1}) {
		t.Fatalf("quota not released: %#v", second)
	}
	mqtt5ITAck(t, sub, second)
	mqtt5ITPing(t, sub)
}

func TestMQTT5IntegrationMaximumPacketSize(t *testing.T) {
	t.Run("tiny connection limit", func(t *testing.T) {
		addr := mqtt5ITBroker(t, nil)
		conn := mqtt5ITDial(t, addr)
		maximum := uint32(2)
		mqtt5ITWrite(t, conn, &mqtt5.Connect{ProtocolName: "MQTT", ProtocolVersion: 5, ClientID: "tiny", CleanStart: true, Properties: &mqtt5.Properties{MaximumPacketSize: &maximum}})
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		wire, err := io.ReadAll(conn)
		if err != nil {
			t.Fatal(err)
		}
		if len(wire) > int(maximum) {
			t.Fatalf("server sent %d bytes beyond peer limit %d: %x", len(wire), maximum, wire)
		}
	})
	for _, extra := range []int{0, 1} {
		t.Run(fmt.Sprintf("publish_boundary_plus_%d", extra), func(t *testing.T) {
			addr := mqtt5ITBroker(t, nil)
			maximum := uint32(64)
			sub := mqtt5ITClient(t, addr, "subscriber", func(c *mqtt5.Connect) { c.Properties.MaximumPacketSize = &maximum })
			pub := mqtt5ITClient(t, addr, "publisher", nil)
			mqtt5ITSubscribe(t, sub, 1, mqtt5.SubOptions{Topic: "size", QoS: 1}, nil)
			packet := &mqtt5.Publish{Topic: "size", Payload: bytes.Repeat([]byte{'x'}, 53+extra), QoS: 1, PacketID: 1, Properties: &mqtt5.Properties{}}
			var encoded bytes.Buffer
			if _, err := packet.WriteTo(&encoded); err != nil {
				t.Fatal(err)
			}
			if encoded.Len() != int(maximum)+extra {
				t.Fatalf("test fixture size %d", encoded.Len())
			}
			if extra == 0 {
				mqtt5ITPublish(t, pub, packet)
				_ = sub.SetReadDeadline(time.Now().Add(2 * time.Second))
				// Capture the complete frame independently of broker frame parsing.
				capture := &bytes.Buffer{}
				cp, err := mqtt5.ReadPacket(io.TeeReader(sub, capture))
				if err != nil {
					t.Fatal(err)
				}
				got, ok := cp.Content.(*mqtt5.Publish)
				if !ok || capture.Len() != int(maximum) || !bytes.Equal(got.Payload, packet.Payload) {
					t.Fatalf("exact boundary delivery: %T, %d bytes", cp.Content, capture.Len())
				}
				mqtt5ITAck(t, sub, got)
				mqtt5ITPing(t, sub)
			} else {
				mqtt5ITPublish(t, pub, packet)
				mqtt5ITQuiet(t, sub)
				mqtt5ITPing(t, sub)
				mqtt5ITPing(t, pub)
			}
		})
	}
}

func TestMQTT5IntegrationOversizeSubscriberIsolation(t *testing.T) {
	addr := mqtt5ITBroker(t, nil)
	maximum := uint32(64)
	receiveMaximum := uint16(1)
	sub := mqtt5ITClient(t, addr, "small-subscriber", func(c *mqtt5.Connect) {
		c.Properties.MaximumPacketSize = &maximum
		c.Properties.ReceiveMaximum = &receiveMaximum
	})
	healthy := mqtt5ITClient(t, addr, "healthy-subscriber", nil)
	pub := mqtt5ITClient(t, addr, "publisher", nil)
	for _, conn := range []net.Conn{sub, healthy} {
		mqtt5ITSubscribe(t, conn, 1, mqtt5.SubOptions{Topic: "isolation", QoS: 1}, nil)
	}
	payload := bytes.Repeat([]byte{'x'}, 128)
	// A consumer's size limit must not turn a valid publication into a publisher disconnect.
	mqtt5ITPublish(t, pub, &mqtt5.Publish{Topic: "isolation", Payload: payload, QoS: 1, PacketID: 1})
	mqtt5ITPing(t, pub)
	got := mqtt5ITRead[*mqtt5.Publish](t, healthy)
	if !bytes.Equal(got.Payload, payload) {
		t.Fatalf("healthy subscriber delivery: %#v", got)
	}
	mqtt5ITAck(t, healthy, got)
	mqtt5ITQuiet(t, sub)
	mqtt5ITPing(t, sub)
	// Dropping an oversized packet must not consume the only receive slot.
	mqtt5ITPublish(t, pub, &mqtt5.Publish{Topic: "isolation", Payload: []byte("fits"), QoS: 1, PacketID: 2})
	for _, conn := range []net.Conn{sub, healthy} {
		got := mqtt5ITRead[*mqtt5.Publish](t, conn)
		if string(got.Payload) != "fits" || got.QoS != 1 {
			t.Fatalf("post-drop delivery: %#v", got)
		}
		mqtt5ITAck(t, conn, got)
		mqtt5ITPing(t, conn)
	}
}

func TestMQTT5IntegrationZeroExpiry(t *testing.T) {
	addr := mqtt5ITBroker(t, nil)
	sub := mqtt5ITClient(t, addr, "subscriber", nil)
	pub := mqtt5ITClient(t, addr, "publisher", nil)
	mqtt5ITSubscribe(t, sub, 1, mqtt5.SubOptions{Topic: "expiry", QoS: 1}, nil)
	zero := uint32(0)
	mqtt5ITPublish(t, pub, &mqtt5.Publish{Topic: "expiry", Payload: []byte("expired"), QoS: 1, PacketID: 1, Retain: true, Properties: &mqtt5.Properties{MessageExpiry: &zero}})
	mqtt5ITQuiet(t, sub)
	late := mqtt5ITClient(t, addr, "late", nil)
	mqtt5ITSubscribe(t, late, 1, mqtt5.SubOptions{Topic: "expiry", QoS: 1}, nil)
	mqtt5ITQuiet(t, late)
	// A fresh publication verifies that silence was expiry, not a broken route.
	mqtt5ITPublish(t, pub, &mqtt5.Publish{Topic: "expiry", Payload: []byte("fresh"), QoS: 1, PacketID: 2})
	for _, conn := range []net.Conn{sub, late} {
		got := mqtt5ITRead[*mqtt5.Publish](t, conn)
		if string(got.Payload) != "fresh" {
			t.Fatalf("expiry delivery: %#v", got)
		}
		mqtt5ITAck(t, conn, got)
	}
}

func TestMQTT5IntegrationDisconnectWill(t *testing.T) {
	for _, reason := range []byte{0, 4, 0x80} {
		t.Run(fmt.Sprint(reason), func(t *testing.T) {
			addr := mqtt5ITBroker(t, nil)
			sub := mqtt5ITClient(t, addr, "subscriber", nil)
			mqtt5ITSubscribe(t, sub, 1, mqtt5.SubOptions{Topic: "will", QoS: 1}, nil)
			pub := mqtt5ITClient(t, addr, "publisher", func(c *mqtt5.Connect) {
				c.WillFlag, c.WillQOS, c.WillTopic, c.WillMessage = true, 1, "will", []byte("gone")
				c.WillProperties = &mqtt5.Properties{User: []mqtt5.User{{Key: "source", Value: "will"}}}
			})
			mqtt5ITWrite(t, pub, &mqtt5.Disconnect{ReasonCode: reason, Properties: &mqtt5.Properties{}})
			if reason == 0 {
				mqtt5ITQuiet(t, sub)
				mqtt5ITPing(t, sub)
				return
			}
			got := mqtt5ITRead[*mqtt5.Publish](t, sub)
			if got.Topic != "will" || string(got.Payload) != "gone" || got.QoS != 1 {
				t.Fatalf("will delivery: %#v", got)
			}
			if !reflect.DeepEqual(got.Properties.User, []mqtt5.User{{Key: "source", Value: "will"}}) {
				t.Fatalf("will properties lost: %#v", got.Properties)
			}
			mqtt5ITAck(t, sub, got)
		})
	}
}

func mqtt5ITAppendVBI(dst []byte, value int) []byte {
	for {
		digit := byte(value % 128)
		value /= 128
		if value != 0 {
			digit |= 0x80
		}
		dst = append(dst, digit)
		if value == 0 {
			return dst
		}
	}
}

func TestMQTT5IntegrationRawWillProperties(t *testing.T) {
	for _, expiry := range []uint32{60, 0} {
		t.Run(fmt.Sprintf("expiry_%d", expiry), func(t *testing.T) {
			addr := mqtt5ITBroker(t, nil)
			sub := mqtt5ITClient(t, addr, "subscriber", nil)
			mqtt5ITSubscribe(t, sub, 1, mqtt5.SubOptions{Topic: "raw/will", QoS: 1}, nil)
			pub := mqtt5ITDial(t, addr)
			appendString := func(dst []byte, value string) []byte {
				dst = binary.BigEndian.AppendUint16(dst, uint16(len(value)))
				return append(dst, value...)
			}
			// Paho v0.23.0 Pack(CONNECT) omits ContentType and MessageExpiry
			// from Will Properties. Encode those properties directly on the wire.
			properties := appendString([]byte{mqtt5.PropContentType}, "text/plain")
			properties = append(properties, mqtt5.PropMessageExpiry)
			properties = binary.BigEndian.AppendUint32(properties, expiry)
			// MQTT 5, Clean Start, retained QoS 1 will, no CONNECT properties.
			body := []byte{0, 4, 'M', 'Q', 'T', 'T', 5, 0x2e, 0, 0, 0}
			body = appendString(body, "raw-will-publisher")
			body = mqtt5ITAppendVBI(body, len(properties))
			body = append(body, properties...)
			body = appendString(body, "raw/will")
			body = appendString(body, "gone")
			frame := mqtt5ITAppendVBI([]byte{0x10}, len(body))
			frame = append(frame, body...)
			decoded, err := mqtt5.ReadPacket(bytes.NewReader(frame))
			if err != nil {
				t.Fatalf("invalid raw CONNECT fixture: %v", err)
			}
			will := decoded.Content.(*mqtt5.Connect).WillProperties
			if will.ContentType != "text/plain" || will.MessageExpiry == nil || *will.MessageExpiry != expiry {
				t.Fatalf("raw fixture properties: %#v", will)
			}
			_ = pub.SetWriteDeadline(time.Now().Add(2 * time.Second))
			if n, err := pub.Write(frame); err != nil || n != len(frame) {
				t.Fatalf("raw CONNECT write: n=%d err=%v", n, err)
			}
			if ack := mqtt5ITRead[*mqtt5.Connack](t, pub); ack.ReasonCode != 0 {
				t.Fatalf("raw CONNECT rejected: %#v", ack)
			}
			mqtt5ITWrite(t, pub, &mqtt5.Disconnect{ReasonCode: 4, Properties: &mqtt5.Properties{}})
			checkWill := func(conn net.Conn, retained bool) {
				t.Helper()
				if expiry == 0 {
					mqtt5ITQuiet(t, conn)
					mqtt5ITPing(t, conn)
					return
				}
				got := mqtt5ITRead[*mqtt5.Publish](t, conn)
				if got.Topic != "raw/will" || string(got.Payload) != "gone" || got.QoS != 1 || got.Retain != retained {
					t.Fatalf("raw will delivery: %#v", got)
				}
				props := got.Properties
				if props.ContentType != "text/plain" || props.MessageExpiry == nil || *props.MessageExpiry == 0 || *props.MessageExpiry > expiry {
					t.Fatalf("raw will properties: %#v", props)
				}
				mqtt5ITAck(t, conn, got)
			}
			checkWill(sub, false)
			late := mqtt5ITClient(t, addr, "late-subscriber", nil)
			mqtt5ITSubscribe(t, late, 1, mqtt5.SubOptions{Topic: "raw/will", QoS: 1}, nil)
			checkWill(late, true)
		})
	}
}
