package broker

import (
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

func TestSharedAndRetainedMQTT(t *testing.T) {
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
	read := func(c net.Conn) packets.ControlPacket {
		t.Helper()
		_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
		p, err := packets.ReadPacket(c)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	write := func(c net.Conn, p packets.ControlPacket) {
		t.Helper()
		if err := p.Write(c); err != nil {
			t.Fatal(err)
		}
	}
	connect := func(id string) net.Conn {
		t.Helper()
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		p := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
		p.ProtocolName, p.ProtocolVersion, p.CleanSession, p.ClientIdentifier = "MQTT", 4, true, id
		write(c, p)
		if a, ok := read(c).(*packets.ConnackPacket); !ok || a.ReturnCode != 0 {
			t.Fatal("connect failed")
		}
		return c
	}
	subscribe := func(c net.Conn, filter string, qos byte) {
		t.Helper()
		p := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
		p.MessageID, p.Topics, p.Qoss = 1, []string{filter}, []byte{qos}
		write(c, p)
		if a, ok := read(c).(*packets.SubackPacket); !ok || len(a.ReturnCodes) != 1 || a.ReturnCodes[0] != qos {
			t.Fatal("subscribe failed")
		}
	}
	a, b1, c, ordinary, pub := connect("share-a"), connect("share-b"), connect("share-c"), connect("normal"), connect("publisher")
	subscribe(a, "$share/g1/events/#", 0)
	subscribe(b1, "$share/g1/events/#", 0)
	subscribe(c, "$share/g2/events/#", 0)
	subscribe(ordinary, "events/#", 0)
	check := func(label string) {
		t.Helper()
		for _, conn := range []net.Conn{c, ordinary} {
			if _, ok := read(conn).(*packets.PublishPacket); !ok {
				t.Fatalf("%s: missing delivery", label)
			}
		}
		count := 0
		for _, conn := range []net.Conn{a, b1} {
			_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
			p, err := packets.ReadPacket(conn)
			if err == nil {
				if _, ok := p.(*packets.PublishPacket); !ok {
					t.Fatalf("unexpected packet %T", p)
				}
				count++
			} else if e, ok := err.(net.Error); !ok || !e.Timeout() {
				t.Fatal(err)
			}
		}
		if count != 1 {
			t.Fatalf("%s: group got %d deliveries", label, count)
		}
	}
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Payload = "events/a", []byte("one")
	write(pub, p)
	check("client publish")
	b.PublishMessage(p)
	check("broker publish")
	b.receiveClusterMessage("peer", []byte(`{"Topic":"events/a","Payload":"b25l","QoS":0,"Retain":false}`))
	check("cluster receive")
	ret := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	ret.TopicName, ret.Payload, ret.Qos, ret.Retain = "retained/a", []byte("saved"), 2, true
	b.PublishMessage(ret)
	r := connect("retained-reader")
	subscribe(r, "retained/#", 1)
	got, ok := read(r).(*packets.PublishPacket)
	if !ok || got.Qos != 1 || got.MessageID == 0 || !got.Retain || string(got.Payload) != "saved" {
		t.Fatalf("retained delivery: %#v", got)
	}
	value, ok := b.clients.Load("retained-reader")
	if !ok {
		t.Fatal("client missing")
	}
	cl := value.(*client)
	cl.inflightMu.RLock()
	entry := cl.inflight[got.MessageID]
	cl.inflightMu.RUnlock()
	if entry == nil {
		t.Fatal("retained delivery not inflight")
	}
	ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
	ack.MessageID = got.MessageID
	write(r, ack)
	for deadline := time.Now().Add(time.Second); ; {
		cl.inflightMu.RLock()
		_, exists := cl.inflight[got.MessageID]
		cl.inflightMu.RUnlock()
		if !exists {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("PUBACK did not clear inflight")
		}
		time.Sleep(time.Millisecond)
	}
	r0 := connect("retained-qos0")
	subscribe(r0, "retained/#", 0)
	p0, ok := read(r0).(*packets.PublishPacket)
	if !ok || p0.Qos != 0 || p0.MessageID != 0 || !p0.Retain {
		t.Fatalf("qos 0 retained delivery: %#v", p0)
	}
	r2 := connect("retained-qos2")
	subscribe(r2, "retained/#", 2)
	p2, ok := read(r2).(*packets.PublishPacket)
	if !ok || p2.Qos != 2 || p2.MessageID == 0 || !p2.Retain {
		t.Fatalf("qos 2 retained delivery: %#v", p2)
	}
	rec := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
	rec.MessageID = p2.MessageID
	write(r2, rec)
	rel, ok := read(r2).(*packets.PubrelPacket)
	if !ok || rel.MessageID != p2.MessageID {
		t.Fatalf("PUBREL: %#v", rel)
	}
	comp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
	comp.MessageID = p2.MessageID
	write(r2, comp)
}
