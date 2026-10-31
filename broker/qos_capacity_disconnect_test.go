package broker

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/fhmq/hmq/v4/broker/packets"
)

func TestMQTTQoS2CapacityDisconnect(t *testing.T) {
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
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	connect := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	connect.ProtocolName, connect.ProtocolVersion, connect.CleanSession, connect.ClientIdentifier = "MQTT", 4, true, "qos2-capacity"
	if err := connect.Write(conn); err != nil {
		t.Fatal(err)
	}
	read := func() (packets.ControlPacket, error) {
		t.Helper()
		if err := conn.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		return packets.ReadPacket(conn)
	}
	p, err := read()
	if err != nil {
		t.Fatal(err)
	}
	if ack, ok := p.(*packets.ConnackPacket); !ok || ack.ReturnCode != 0 {
		t.Fatalf("CONNACK: %#v", p)
	}
	var c *client
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		if v, ok := b.clients.Load("qos2-capacity"); ok {
			c = v.(*client)
			break
		}
		time.Sleep(time.Millisecond)
	}
	if c == nil {
		t.Fatal("client not registered")
	}
	c.awaitingRelMu.Lock()
	c.maxAwaitingRel = 1
	c.awaitingRelMu.Unlock()
	pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	pub.TopicName, pub.Qos, pub.MessageID, pub.Payload = "capacity/test", 2, 1, []byte("first")
	for i := 0; i < 2; i++ {
		if err := pub.Write(conn); err != nil {
			t.Fatal(err)
		}
		p, err := read()
		if err != nil {
			t.Fatal(err)
		}
		if ack, ok := p.(*packets.PubrecPacket); !ok || ack.MessageID != 1 {
			t.Fatalf("PUBREC: %#v", p)
		}
		pub.Dup = true
	}
	pub.MessageID, pub.Dup, pub.Payload = 2, false, []byte("second")
	if err := pub.Write(conn); err != nil {
		t.Fatal(err)
	}
	p, err = read()
	if err == nil {
		t.Fatalf("expected disconnect, got packet %#v", p)
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("connection remained open: %v", err)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF from server close, got %v", err)
	}
}
