package broker

import (
	"net"
	"testing"
	"time"

	"github.com/fhmq/hmq/v4/broker/packets"
)

func TestMQTTSessionPresent(t *testing.T) {
	b, err := NewBroker(&Config{Host: "127.0.0.1", Port: "0"})
	if err != nil {
		t.Fatal(err)
	}
	b.Start()
	t.Cleanup(b.Close)
	var addr string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
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
	connect := func(clean, present bool) {
		t.Helper()
		conn, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		msg := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
		msg.ProtocolName = "MQTT"
		msg.ProtocolVersion = 4
		msg.ClientIdentifier = "session-present-test"
		msg.CleanSession = clean
		if err := msg.Write(conn); err != nil {
			t.Fatal(err)
		}
		p, err := packets.ReadPacket(conn)
		if err != nil {
			t.Fatal(err)
		}
		ack, ok := p.(*packets.ConnackPacket)
		if !ok || ack.ReturnCode != packets.Accepted || ack.SessionPresent != present {
			t.Fatalf("clean=%v: unexpected CONNACK %#v, want present=%v", clean, p, present)
		}
		if err := packets.NewControlPacket(packets.Disconnect).Write(conn); err != nil {
			t.Fatal(err)
		}
	}
	connect(false, false)
	connect(false, true)
	connect(true, false)
	connect(false, false)
	connect(false, true)
}
