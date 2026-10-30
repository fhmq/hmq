package broker

import (
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

func TestMQTTInboundQoS2SessionReconnect(t *testing.T) {
	b, err := NewBroker(&Config{Host: "127.0.0.1", Port: "0"})
	if err != nil {
		t.Fatal(err)
	}
	b.Start()
	t.Cleanup(b.Close)
	var addr string
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		b.lifecycleMu.Lock()
		for l := range b.listeners {
			addr = l.Addr().String()
		}
		b.lifecycleMu.Unlock()
		if addr != "" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if addr == "" {
		t.Fatal("listener did not start")
	}
	connect := func(id string, clean, present bool) net.Conn {
		t.Helper()
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		c.SetDeadline(time.Now().Add(5 * time.Second))
		p := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
		p.ProtocolName = "MQTT"
		p.ProtocolVersion = 4
		p.ClientIdentifier = id
		p.CleanSession = clean
		if err := p.Write(c); err != nil {
			t.Fatal(err)
		}
		a, err := packets.ReadPacket(c)
		if err != nil {
			t.Fatal(err)
		}
		ack, ok := a.(*packets.ConnackPacket)
		if !ok || ack.ReturnCode != packets.Accepted || ack.SessionPresent != present {
			t.Fatalf("unexpected connack: %#v", a)
		}
		return c
	}
	read := func(c net.Conn) packets.ControlPacket {
		t.Helper()
		p, err := packets.ReadPacket(c)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	sub := connect("qos2-sub", true, false)
	s := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	s.MessageID = 1
	s.Topics = []string{"qos2/session"}
	s.Qoss = []byte{0}
	if err := s.Write(sub); err != nil {
		t.Fatal(err)
	}
	read(sub)
	pub := func(c net.Conn, dup bool) {
		t.Helper()
		p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		p.Qos = 2
		p.Dup = dup
		p.MessageID = 42
		p.TopicName = "qos2/session"
		p.Payload = []byte("hello")
		if err := p.Write(c); err != nil {
			t.Fatal(err)
		}
		if _, ok := read(c).(*packets.PubrecPacket); !ok {
			t.Fatal("expected PUBREC")
		}
	}
	c := connect("qos2-publisher", false, false)
	pub(c, false)
	if _, ok := read(sub).(*packets.PublishPacket); !ok {
		t.Fatal("expected publish")
	}
	c.Close()
	c = connect("qos2-publisher", false, true)
	pub(c, true)
	sub.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if p, err := packets.ReadPacket(sub); err == nil {
		t.Fatalf("duplicate delivered: %#v", p)
	}
	sub.SetReadDeadline(time.Now().Add(5 * time.Second))
	rel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
	rel.MessageID = 42
	if err := rel.Write(c); err != nil {
		t.Fatal(err)
	}
	if _, ok := read(c).(*packets.PubcompPacket); !ok {
		t.Fatal("expected PUBCOMP")
	}
	pub(c, false)
	if _, ok := read(sub).(*packets.PublishPacket); !ok {
		t.Fatal("expected reused identifier to deliver")
	}
	c.Close()
	c = connect("qos2-publisher", true, false)
	pub(c, false)
	if _, ok := read(sub).(*packets.PublishPacket); !ok {
		t.Fatal("expected clean session to deliver")
	}
}
