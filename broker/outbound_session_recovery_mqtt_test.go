package broker

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/fhmq/hmq/v4/broker/packets"
)

func TestMQTTOutboundSessionRecovery(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		t.Run(fmt.Sprintf("qos%d", qos), func(t *testing.T) {
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
				c.SetDeadline(time.Now().Add(3 * time.Second))
				p := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
				p.ProtocolName, p.ProtocolVersion, p.ClientIdentifier, p.CleanSession = "MQTT", 4, id, clean
				if err := p.Write(c); err != nil {
					t.Fatal(err)
				}
				got, err := packets.ReadPacket(c)
				if err != nil {
					t.Fatal(err)
				}
				ack, ok := got.(*packets.ConnackPacket)
				if !ok || ack.ReturnCode != packets.Accepted || ack.SessionPresent != present {
					t.Fatalf("unexpected CONNACK: %#v, want session present %v", got, present)
				}
				return c
			}
			sub := connect("recovery-subscriber", false, false)
			s := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
			s.MessageID, s.Topics, s.Qoss = 1, []string{"recovery/topic"}, []byte{qos}
			if err := s.Write(sub); err != nil {
				t.Fatal(err)
			}
			if p, err := packets.ReadPacket(sub); err != nil {
				t.Fatal(err)
			} else if _, ok := p.(*packets.SubackPacket); !ok {
				t.Fatalf("expected SUBACK, got %T", p)
			}
			pub := connect("recovery-publisher", true, false)
			p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			p.TopicName, p.Payload, p.Qos, p.MessageID = "recovery/topic", []byte("payload"), qos, 17
			if err := p.Write(pub); err != nil {
				t.Fatal(err)
			}
			got, err := packets.ReadPacket(sub)
			if err != nil {
				t.Fatal(err)
			}
			delivery, ok := got.(*packets.PublishPacket)
			if !ok || delivery.Qos != qos || delivery.MessageID == 0 {
				t.Fatalf("unexpected delivery: %#v", got)
			}
			id := delivery.MessageID
			if qos == 2 {
				rec := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
				rec.MessageID = id
				if err := rec.Write(sub); err != nil {
					t.Fatal(err)
				}
				got, err = packets.ReadPacket(sub)
				if err != nil {
					t.Fatal(err)
				}
				rel, ok := got.(*packets.PubrelPacket)
				if !ok || rel.MessageID != id {
					t.Fatalf("expected PUBREL %d, got %#v", id, got)
				}
			}
			sub.Close()
			// Wait for the old connection to be removed before reconnecting.
			for deadline := time.Now().Add(3 * time.Second); ; {
				if _, ok := b.clients.Load("recovery-subscriber"); !ok {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("old connection did not close")
				}
				time.Sleep(time.Millisecond)
			}
			sub = connect("recovery-subscriber", false, true)
			got, err = packets.ReadPacket(sub)
			if err != nil {
				t.Fatalf("missing recovered QoS %d packet (id %d): %v", qos, id, err)
			}
			if qos == 1 {
				p, ok := got.(*packets.PublishPacket)
				if !ok || p.MessageID != id || !p.Dup || p.Qos != qos || string(p.Payload) != "payload" {
					t.Fatalf("unexpected recovered PUBLISH: %#v", got)
				}
				ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
				ack.MessageID = id
				if err := ack.Write(sub); err != nil {
					t.Fatal(err)
				}
			} else {
				rel, ok := got.(*packets.PubrelPacket)
				if !ok || rel.MessageID != id {
					t.Fatalf("unexpected recovered PUBREL: %#v", got)
				}
				comp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
				comp.MessageID = id
				if err := comp.Write(sub); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
