package broker

import (
	"net"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/fhmq/hmq/v4/broker/packets"
)

func TestMQTTQoS2DuplicatePublishAndPubrel(t *testing.T) {
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
	sub := mqtt.NewClient(mqtt.NewClientOptions().AddBroker("tcp://" + addr).SetClientID("qos2-sub").SetAutoReconnect(false))
	token := sub.Connect()
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		t.Fatalf("connect subscriber: %v", token.Error())
	}
	t.Cleanup(func() { sub.Disconnect(100) })
	messages := make(chan string, 10)
	token = sub.Subscribe("qos2/test", 1, func(_ mqtt.Client, m mqtt.Message) { messages <- string(m.Payload()) })
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		t.Fatalf("subscribe: %v", token.Error())
	}
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	connect := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	connect.ProtocolName, connect.ProtocolVersion, connect.CleanSession, connect.ClientIdentifier = "MQTT", 4, true, "qos2-pub"
	if err := connect.Write(conn); err != nil {
		t.Fatal(err)
	}
	read := func() packets.ControlPacket {
		t.Helper()
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		p, err := packets.ReadPacket(conn)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	if p, ok := read().(*packets.ConnackPacket); !ok || p.ReturnCode != 0 {
		t.Fatal("invalid CONNACK")
	}
	pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	pub.TopicName, pub.Qos, pub.MessageID, pub.Payload = "qos2/test", 2, 17, []byte("first")
	write := func(p packets.ControlPacket) {
		t.Helper()
		if err := p.Write(conn); err != nil {
			t.Fatal(err)
		}
	}
	expectRec := func() {
		t.Helper()
		p, ok := read().(*packets.PubrecPacket)
		if !ok || p.MessageID != 17 {
			t.Fatalf("expected PUBREC 17, got %#v", p)
		}
	}
	expectComp := func() {
		t.Helper()
		p, ok := read().(*packets.PubcompPacket)
		if !ok || p.MessageID != 17 {
			t.Fatalf("expected PUBCOMP 17, got %#v", p)
		}
	}
	write(pub)
	expectRec()
	pub.Dup = true
	write(pub)
	expectRec()
	rel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
	rel.MessageID = 17
	write(rel)
	expectComp()
	write(rel)
	expectComp()
	pub.Dup, pub.Payload = false, []byte("second")
	write(pub)
	expectRec()
	write(rel)
	expectComp()
	for _, want := range []string{"first", "second"} {
		select {
		case got := <-messages:
			if got != want {
				t.Fatalf("delivery = %q, want %q", got, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("missing delivery %q", want)
		}
	}
	select {
	case got := <-messages:
		t.Fatalf("duplicate delivery: %q", got)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestBrokerPublishMessageQoSAndInflight(t *testing.T) {
	b, err := NewBroker(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	c := &client{conn: server, status: Connected, inflight: make(map[uint16]*inflightElem)}
	c.info.clientID = "qos-delivery-test"
	sub := &subscription{client: c, topic: "test", qos: 1}
	c.subMap = map[string]*subscription{"test": sub}
	b.clients.Store(c.info.clientID, c)
	defer b.clients.Delete(c.info.clientID)
	defer func() {
		c.retryTimerLock.Lock()
		if c.retryTimer != nil {
			c.retryTimer.Stop()
		}
		c.retryTimerLock.Unlock()
	}()
	_, err = b.topicsMgr.Subscribe([]byte("test"), 1, sub)
	if err != nil {
		t.Fatal(err)
	}
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Qos, p.MessageID = "test", 2, 42
	read := func() *packets.PublishPacket {
		t.Helper()
		peer.SetReadDeadline(time.Now().Add(time.Second))
		v, err := packets.ReadPacket(peer)
		if err != nil {
			t.Fatal(err)
		}
		return v.(*packets.PublishPacket)
	}
	go b.PublishMessage(p)
	first := read()
	go b.PublishMessage(p)
	second := read()
	if first.Qos != 1 || second.Qos != 1 || first.MessageID == 0 || first.MessageID == second.MessageID {
		t.Fatalf("unexpected deliveries: %#v %#v", first, second)
	}
	if p.Qos != 2 || p.MessageID != 42 {
		t.Fatal("original packet changed")
	}
	c.inflightMu.RLock()
	defer c.inflightMu.RUnlock()
	if len(c.inflight) != 2 {
		t.Fatalf("inflight count = %d", len(c.inflight))
	}
}

func TestQoS2AwaitingDuplicateAtCapacity(t *testing.T) {
	c := &client{awaitingRel: make(map[uint16]int64), maxAwaitingRel: 1}
	if err := c.registerPublishPacketId(7); err != nil {
		t.Fatal(err)
	}
	if err := c.registerPublishPacketId(7); err == nil || err.Error() != "RC_PACKET_IDENTIFIER_IN_USE" {
		t.Fatalf("duplicate: %v", err)
	}
	if err := c.registerPublishPacketId(8); err == nil {
		t.Fatal("expected capacity error")
	}
	if err := c.pubRel(7); err != nil {
		t.Fatal(err)
	}
	if err := c.registerPublishPacketId(7); err != nil {
		t.Fatal(err)
	}
}

func TestWrongAcknowledgementDoesNotRemoveInflight(t *testing.T) {
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.Qos, p.MessageID = 2, 7
	c := &client{inflight: map[uint16]*inflightElem{7: {status: Publish, packet: p}}}
	ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
	ack.MessageID = 7
	ProcessMessage(&Message{client: c, packet: ack})
	comp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
	comp.MessageID = 7
	ProcessMessage(&Message{client: c, packet: comp})
	if _, ok := c.inflight[7]; !ok {
		t.Fatal("incorrect acknowledgement removed QoS2 message")
	}
}
