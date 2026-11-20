package e2e

import (
	"fmt"
	"net"
	"testing"
	"time"

	mqtt5 "github.com/eclipse/paho.golang/packets"
	"github.com/fhmq/hmq/v4/broker/packets"
)

func qos2Connect(t *testing.T, node, id string, present bool) net.Conn {
	t.Helper()
	p := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	p.ProtocolName, p.ProtocolVersion, p.ClientIdentifier = "MQTT", 4, id
	var last error
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", ports[node]), time.Second)
		if err == nil {
			_ = c.SetDeadline(time.Now().Add(8 * time.Second))
			if err = p.Write(c); err == nil {
				var got packets.ControlPacket
				got, err = packets.ReadPacket(c)
				if err == nil {
					a, ok := got.(*packets.ConnackPacket)
					if !ok || a.ReturnCode != 0 || a.SessionPresent != present {
						_ = c.Close()
						t.Fatalf("%s CONNACK: %#v, want Session Present=%v", id, got, present)
					}
					t.Cleanup(func() { _ = c.Close() })
					return c
				}
			}
			_ = c.Close()
		}
		last = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("CONNECT %s on %s: %v", id, node, last)
	return nil
}

func qos2Write(t *testing.T, c net.Conn, p packets.ControlPacket) {
	t.Helper()
	_ = c.SetWriteDeadline(time.Now().Add(8 * time.Second))
	if err := p.Write(c); err != nil {
		t.Fatal(err)
	}
}

func qos2Read[T packets.ControlPacket](t *testing.T, c net.Conn) T {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(8 * time.Second))
	p, err := packets.ReadPacket(c)
	got, ok := p.(T)
	if err != nil || !ok {
		t.Fatalf("read %T: got %#v, %v", got, p, err)
	}
	return got
}

func qos2Recovery(t *testing.T, start string, migrate, restart func() string) {
	id := fmt.Sprintf("compose-qos2-%d", time.Now().UnixNano())
	topic := id + "/message"
	pub := qos2Connect(t, start, id+"-pub", false)
	sub := qos2Connect(t, start, id+"-sub", false)
	s := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	s.MessageID, s.Topics, s.Qoss = 1, []string{id + "/+"}, []byte{2}
	qos2Write(t, sub, s)
	if a := qos2Read[*packets.SubackPacket](t, sub); len(a.ReturnCodes) != 1 || a.ReturnCodes[0] != 2 {
		t.Fatalf("SUBACK %#v", a)
	}
	_ = sub.Close()
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.MessageID, p.Qos, p.TopicName, p.Payload = 25, 2, topic, []byte("original")
	qos2Write(t, pub, p)
	if a := qos2Read[*packets.PubrecPacket](t, pub); a.MessageID != 25 {
		t.Fatal(a)
	}
	_ = pub.Close()
	next := migrate()
	pub = qos2Connect(t, next, id+"-pub", true)
	p.Dup, p.Payload = true, []byte("must not overwrite")
	qos2Write(t, pub, p)
	if a := qos2Read[*packets.PubrecPacket](t, pub); a.MessageID != 25 {
		t.Fatal(a)
	}
	rel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
	rel.MessageID = 25
	qos2Write(t, pub, rel)
	if a := qos2Read[*packets.PubcompPacket](t, pub); a.MessageID != 25 {
		t.Fatal(a)
	}
	_ = pub.Close()
	sub = qos2Connect(t, next, id+"-sub", true)
	delivery := qos2Read[*packets.PublishPacket](t, sub)
	if delivery.Qos != 2 || string(delivery.Payload) != "original" || delivery.TopicName != topic {
		t.Fatalf("delivery %#v", delivery)
	}
	_ = sub.Close()
	sub = qos2Connect(t, next, id+"-sub", true)
	if retry := qos2Read[*packets.PublishPacket](t, sub); !retry.Dup || retry.MessageID != delivery.MessageID {
		t.Fatalf("replayed PUBLISH %#v", retry)
	}
	rec := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
	rec.MessageID = delivery.MessageID
	qos2Write(t, sub, rec)
	if a := qos2Read[*packets.PubrelPacket](t, sub); a.MessageID != rec.MessageID {
		t.Fatal(a)
	}
	_ = sub.Close()
	next = restart()
	pub = qos2Connect(t, next, id+"-pub", true)
	qos2Write(t, pub, rel)
	if a := qos2Read[*packets.PubcompPacket](t, pub); a.MessageID != 25 {
		t.Fatal(a)
	}
	sub = qos2Connect(t, next, id+"-sub", true)
	if a := qos2Read[*packets.PubrelPacket](t, sub); a.MessageID != rec.MessageID {
		t.Fatal(a)
	}
	comp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
	comp.MessageID = rec.MessageID
	qos2Write(t, sub, comp)
	// PINGRESP follows PUBCOMP processing on the same connection.
	qos2Write(t, sub, packets.NewControlPacket(packets.Pingreq))
	qos2Read[*packets.PingrespPacket](t, sub)
	_ = sub.Close()
	sub = qos2Connect(t, next, id+"-sub", true)
	_ = sub.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	if p, err := packets.ReadPacket(sub); !isTimeout(err) {
		t.Fatalf("expected no duplicate delivery, got %#v, %v", p, err)
	}
}

func isTimeout(err error) bool {
	e, ok := err.(net.Error)
	return ok && e.Timeout()
}

func TestStandaloneQoS2Recovery(t *testing.T) {
	enabled(t)
	restart := func() string {
		compose(t, "restart", "-t", "3", "single")
		return "single"
	}
	qos2Recovery(t, "single", func() string {
		compose(t, "kill", "-s", "SIGKILL", "single")
		compose(t, "start", "single")
		return "single"
	}, restart)
}

func TestQuorumQoS2Recovery(t *testing.T) {
	enabled(t)
	nodes := []string{"a", "b", "c"}
	first := leader(t, nodes)
	t.Cleanup(func() { compose(t, "start", nodes[0], nodes[1], nodes[2]) })
	qos2Recovery(t, first, func() string {
		compose(t, "stop", "-t", "3", first)
		var survivors []string
		for _, node := range nodes {
			if node != first {
				survivors = append(survivors, node)
			}
		}
		next := leader(t, survivors)
		t.Logf("resuming publisher after leader change %s -> %s", first, next)
		return next
	}, func() string {
		compose(t, "stop", "-t", "3", nodes[0], nodes[1], nodes[2])
		compose(t, "start", nodes[0], nodes[1], nodes[2])
		leader(t, nodes)
		t.Logf("resuming subscriber on %s after full cluster restart", first)
		return first
	})
}

func TestMQTT5QoS2SessionResume(t *testing.T) {
	enabled(t)
	for _, pair := range [][2]string{{"single", "single"}, {"a", "b"}} {
		t.Run(pair[0]+"-"+pair[1], func(t *testing.T) {
			id := fmt.Sprintf("mqtt5-qos2-%d", time.Now().UnixNano())
			write := func(c net.Conn, p mqtt5.Packet) {
				t.Helper()
				if _, err := p.WriteTo(c); err != nil {
					t.Fatal(err)
				}
			}
			read := func(c net.Conn) mqtt5.Packet {
				t.Helper()
				p, err := mqtt5.ReadPacket(c)
				if err != nil {
					t.Fatal(err)
				}
				return p.Content
			}
			connect := func(node, clientID string, clean bool) net.Conn {
				t.Helper()
				c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", ports[node]), 3*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = c.Close() })
				_ = c.SetDeadline(time.Now().Add(8 * time.Second))
				expiry := ^uint32(0)
				write(c, &mqtt5.Connect{ProtocolName: "MQTT", ProtocolVersion: 5, ClientID: clientID, CleanStart: clean, Properties: &mqtt5.Properties{SessionExpiryInterval: &expiry}})
				got := read(c)
				a, ok := got.(*mqtt5.Connack)
				if !ok || a.ReasonCode != 0 || a.SessionPresent == clean ||
					a.Properties.SessionExpiryInterval == nil || *a.Properties.SessionExpiryInterval != expiry || a.Properties.MaximumQOS != nil {
					t.Fatalf("CONNACK %#v", got)
				}
				return c
			}
			sub := connect(pair[0], id+"-sub", true)
			write(sub, &mqtt5.Subscribe{PacketID: 2, Properties: &mqtt5.Properties{}, Subscriptions: []mqtt5.SubOptions{{Topic: id, QoS: 2}}})
			got := read(sub)
			if a, ok := got.(*mqtt5.Suback); !ok || len(a.Reasons) != 1 || a.Reasons[0] != 2 {
				t.Fatalf("expected QoS2 SUBACK, got %#v", got)
			}
			_ = sub.Close()
			pub := connect(pair[0], id, true)
			write(pub, &mqtt5.Publish{PacketID: 3, QoS: 2, Topic: id, Payload: []byte("persisted"), Properties: &mqtt5.Properties{ContentType: "text/plain"}})
			got = read(pub)
			if a, ok := got.(*mqtt5.Pubrec); !ok || a.PacketID != 3 || a.ReasonCode >= 0x80 {
				t.Fatalf("expected PUBREC, got %#v", got)
			}
			_ = pub.Close()
			pub = connect(pair[1], id, false)
			write(pub, &mqtt5.Pubrel{PacketID: 3, Properties: &mqtt5.Properties{}})
			got = read(pub)
			if a, ok := got.(*mqtt5.Pubcomp); !ok || a.PacketID != 3 || a.ReasonCode >= 0x80 {
				t.Fatalf("expected PUBCOMP, got %#v", got)
			}
			sub = connect(pair[1], id+"-sub", false)
			got = read(sub)
			delivery, ok := got.(*mqtt5.Publish)
			if !ok || delivery.QoS != 2 || string(delivery.Payload) != "persisted" || delivery.Properties.ContentType != "text/plain" {
				t.Fatalf("expected original publication and metadata, got %#v", got)
			}
			write(sub, &mqtt5.Pubrec{PacketID: delivery.PacketID, Properties: &mqtt5.Properties{}})
			got = read(sub)
			if a, ok := got.(*mqtt5.Pubrel); !ok || a.PacketID != delivery.PacketID {
				t.Fatalf("expected PUBREL, got %#v", got)
			}
			_ = sub.Close()
			sub = connect(pair[0], id+"-sub", false)
			got = read(sub)
			if a, ok := got.(*mqtt5.Pubrel); !ok || a.PacketID != delivery.PacketID {
				t.Fatalf("expected resumed PUBREL, got %#v", got)
			}
			write(sub, &mqtt5.Pubcomp{PacketID: delivery.PacketID, Properties: &mqtt5.Properties{}})
		})
	}
}
