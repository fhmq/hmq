package broker

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

func logMQTTConnect(t *testing.T, b *Broker, id string, present bool) (net.Conn, <-chan error) {
	t.Helper()
	server, peer := net.Pipe()
	peer.SetDeadline(time.Now().Add(5 * time.Second))
	done := make(chan error, 1)
	go func() { done <- b.handleConnection(CLIENT, server) }()
	sendTestConnect(t, peer, id, false)
	p, err := packets.ReadPacket(peer)
	if err != nil {
		t.Fatal(err)
	}
	if a, ok := p.(*packets.ConnackPacket); !ok || a.SessionPresent != present {
		t.Fatalf("CONNACK: %#v", p)
	}
	t.Cleanup(func() { peer.Close() })
	return peer, done
}

func TestSessionLogMQTTOfflineRestartAndQoS2Phase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.log")
	open := func() *Broker {
		b, err := NewBroker(&Config{ExperimentalSessionLogPath: path})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { b.Close() })
		return b
	}
	b := open()
	c, done := logMQTTConnect(t, b, "durable", false)
	sub := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	sub.MessageID, sub.Topics, sub.Qoss = 1, []string{"persist/+", "$share/g/persist/+"}, []byte{2, 2}
	if err := sub.Write(c); err != nil {
		t.Fatal(err)
	}
	a, err := packets.ReadPacket(c)
	if err != nil {
		t.Fatal(err)
	}
	if ack := a.(*packets.SubackPacket); len(ack.ReturnCodes) != 2 || ack.ReturnCodes[0] != 2 || ack.ReturnCodes[1] != QosFailure {
		t.Fatalf("SUBACK: %#v", ack)
	}
	c.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("disconnect stuck")
	}
	b.Close()
	b = open()
	// A real MQTT QoS1 publisher reaches the recovered offline subscription.
	pub, pubDone := logMQTTConnect(t, b, "publisher", false)
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Payload, p.Qos, p.MessageID = "persist/a", []byte("offline"), 1, 9
	if err := p.Write(pub); err != nil {
		t.Fatal(err)
	}
	if a, err := packets.ReadPacket(pub); err != nil {
		t.Fatal(err)
	} else if _, ok := a.(*packets.PubackPacket); !ok {
		t.Fatalf("PUBACK: %T", a)
	}
	pub.Close()
	<-pubDone
	b.Close()
	b = open()
	c, done = logMQTTConnect(t, b, "durable", true)
	a, err = packets.ReadPacket(c)
	if err != nil {
		t.Fatal(err)
	}
	delivery := a.(*packets.PublishPacket)
	if string(delivery.Payload) != "offline" || !delivery.Dup || delivery.Qos != 1 {
		t.Fatalf("delivery: %#v", delivery)
	}
	ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
	ack.MessageID = delivery.MessageID
	if err := ack.Write(c); err != nil {
		t.Fatal(err)
	}
	if err := packets.NewControlPacket(packets.Pingreq).Write(c); err != nil {
		t.Fatal(err)
	}
	if _, err := packets.ReadPacket(c); err != nil {
		t.Fatal(err)
	}
	c.Close()
	<-done
	state, _, err := b.sessionLog.Get("durable")
	if err != nil || len(state.Inflight) != 0 || len(state.Queue) != 0 {
		t.Fatalf("ACK not durable: %#v %v", state, err)
	}
	c, done = logMQTTConnect(t, b, "durable", true)
	p.Qos, p.Payload = 2, []byte("outbound qos2")
	sent := make(chan error, 1)
	go func() { sent <- b.publishLocal(p) }()
	a, err = packets.ReadPacket(c)
	if err != nil {
		t.Fatal(err)
	}
	delivery = a.(*packets.PublishPacket)
	if delivery.Qos != 2 {
		t.Fatalf("QoS: %d", delivery.Qos)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	// Restart before PUBREC: retransmit PUBLISH with the same ID and DUP.
	c.Close()
	<-done
	b.Close()
	b = open()
	c, done = logMQTTConnect(t, b, "durable", true)
	a, err = packets.ReadPacket(c)
	if err != nil {
		t.Fatal(err)
	}
	replay := a.(*packets.PublishPacket)
	if replay.MessageID != delivery.MessageID || !replay.Dup {
		t.Fatalf("replay: %#v", replay)
	}
	rec := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
	rec.MessageID = replay.MessageID
	if err := rec.Write(c); err != nil {
		t.Fatal(err)
	}
	a, err = packets.ReadPacket(c)
	if err != nil {
		t.Fatal(err)
	}
	if rel, ok := a.(*packets.PubrelPacket); !ok || rel.MessageID != rec.MessageID {
		t.Fatalf("PUBREL: %#v", a)
	}
	c.Close()
	<-done
	b.Close()
	b = open()
	c, done = logMQTTConnect(t, b, "durable", true)
	a, err = packets.ReadPacket(c)
	if err != nil {
		t.Fatal(err)
	}
	if rel, ok := a.(*packets.PubrelPacket); !ok || rel.MessageID != rec.MessageID {
		t.Fatalf("recovered PUBREL: %#v", a)
	}
	comp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
	comp.MessageID = rec.MessageID
	if err := comp.Write(c); err != nil {
		t.Fatal(err)
	}
	if err := packets.NewControlPacket(packets.Pingreq).Write(c); err != nil {
		t.Fatal(err)
	}
	if _, err := packets.ReadPacket(c); err != nil {
		t.Fatal(err)
	}
	c.Close()
	<-done
	state, _, err = b.sessionLog.Get("durable")
	if err != nil || len(state.Inflight) != 0 {
		t.Fatalf("PUBCOMP not durable: %#v %v", state, err)
	}
}
