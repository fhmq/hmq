package broker

import (
	"net"
	"testing"
	"time"

	mqtt5 "github.com/eclipse/paho.golang/packets"
	"github.com/fhmq/hmq/v4/broker/packets"
)

func mqtt5SessionNegotiationBroker(t *testing.T) (*Broker, string) {
	t.Helper()
	b, err := NewBroker(&Config{Host: "127.0.0.1", Port: "0"})
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
			return b, addr
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("broker listener did not start")
	return nil, ""
}

func mqtt5SessionLegacyConnect(t *testing.T, addr, id string, present bool) net.Conn {
	t.Helper()
	conn := mqtt5ITDial(t, addr)
	p := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	p.ProtocolName, p.ProtocolVersion, p.ClientIdentifier = "MQTT", 4, id
	p.CleanSession = false
	mqtt5ITLegacyWrite(t, conn, p)
	ack, ok := mqtt5ITLegacyRead(t, conn).(*packets.ConnackPacket)
	if !ok || ack.ReturnCode != 0 || ack.SessionPresent != present {
		t.Fatalf("legacy CONNACK: %#v, want session present %v", ack, present)
	}
	mqtt5ITLegacyWrite(t, conn, packets.NewControlPacket(packets.Pingreq))
	if _, ok := mqtt5ITLegacyRead(t, conn).(*packets.PingrespPacket); !ok {
		t.Fatal("legacy connection not ready")
	}
	return conn
}

func TestMQTT5SessionNegotiationRejectsExisting(t *testing.T) {
	for _, mode := range []string{"active-mqtt5", "active-persistent-mqtt3", "offline-persistent-mqtt3"} {
		t.Run(mode, func(t *testing.T) {
			b, addr := mqtt5SessionNegotiationBroker(t)
			const id, topic = "existing-session", "session/negotiation"
			var old net.Conn
			if mode == "active-mqtt5" {
				old = mqtt5ITClient(t, addr, id, nil)
				mqtt5ITSubscribe(t, old, 1, mqtt5.SubOptions{Topic: topic}, nil)
			} else {
				old = mqtt5SessionLegacyConnect(t, addr, id, false)
				sub := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
				sub.MessageID, sub.Topics, sub.Qoss = 1, []string{topic}, []byte{0}
				mqtt5ITLegacyWrite(t, old, sub)
				ack, ok := mqtt5ITLegacyRead(t, old).(*packets.SubackPacket)
				if !ok || len(ack.ReturnCodes) != 1 || ack.ReturnCodes[0] != 0 {
					t.Fatalf("SUBACK: %#v", ack)
				}
			}
			value, ok := b.clients.Load(id)
			if !ok {
				t.Fatal("original connection missing")
			}
			original := value.(*client)
			originalSession := original.session
			if mode == "offline-persistent-mqtt3" {
				mqtt5ITLegacyWrite(t, old, packets.NewControlPacket(packets.Disconnect))
				if err := old.Close(); err != nil {
					t.Fatal(err)
				}
				deadline := time.Now().Add(2 * time.Second)
				for {
					if _, active := b.clients.Load(id); !active {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("original client did not disconnect")
					}
					time.Sleep(time.Millisecond)
				}
			}
			_, ack := mqtt5ITConnect(t, addr, id, func(c *mqtt5.Connect) { c.CleanStart = false })
			if ack.ReasonCode != 0x83 || ack.SessionPresent {
				t.Fatalf("expected refusal 0x83 without session present: %#v", ack)
			}
			if mode == "active-mqtt5" {
				if _, err := b.sessionMgr.Get(id); err == nil {
					t.Fatal("ephemeral session unexpectedly saved")
				}
			} else {
				saved, err := b.sessionMgr.Get(id)
				if err != nil || saved != originalSession {
					t.Fatalf("persistent session replaced or deleted: %v", err)
				}
				filters, _, err := saved.Topics()
				if err != nil || len(filters) != 1 || filters[0] != topic {
					t.Fatalf("subscriptions changed: %v, %v", filters, err)
				}
			}
			if mode == "offline-persistent-mqtt3" {
				if _, active := b.clients.Load(id); active {
					t.Fatal("rejected client registered")
				}
				old = mqtt5SessionLegacyConnect(t, addr, id, true)
			} else {
				current, active := b.clients.Load(id)
				if !active || current != original || original.isDisconnected() {
					t.Fatal("rejection displaced original connection")
				}
			}
			publisher := mqtt5ITClient(t, addr, "publisher", nil)
			mqtt5ITPublish(t, publisher, &mqtt5.Publish{Topic: topic, Payload: []byte("still subscribed"), QoS: 1, PacketID: 1})
			if mode == "active-mqtt5" {
				got := mqtt5ITRead[*mqtt5.Publish](t, old)
				if got.Topic != topic || string(got.Payload) != "still subscribed" {
					t.Fatalf("unexpected publication: %#v", got)
				}
				mqtt5ITPing(t, old)
			} else {
				got, ok := mqtt5ITLegacyRead(t, old).(*packets.PublishPacket)
				if !ok || got.TopicName != topic || string(got.Payload) != "still subscribed" {
					t.Fatalf("unexpected publication: %#v", got)
				}
				mqtt5ITLegacyWrite(t, old, packets.NewControlPacket(packets.Pingreq))
				if _, ok := mqtt5ITLegacyRead(t, old).(*packets.PingrespPacket); !ok {
					t.Fatal("original connection no longer usable")
				}
			}
		})
	}
}

func TestMQTT5SessionNegotiationAcceptsFresh(t *testing.T) {
	b, addr := mqtt5SessionNegotiationBroker(t)
	const id = "fresh-session"
	conn, ack := mqtt5ITConnect(t, addr, id, func(c *mqtt5.Connect) {
		c.CleanStart = false
		expiry := uint32(3600)
		c.Properties.SessionExpiryInterval = &expiry
	})
	if ack.ReasonCode != 0 || ack.SessionPresent || ack.Properties == nil || ack.Properties.SessionExpiryInterval == nil || *ack.Properties.SessionExpiryInterval != 0 {
		t.Fatalf("expected new ephemeral session: %#v", ack)
	}
	mqtt5ITPing(t, conn)
	if _, active := b.clients.Load(id); !active {
		t.Fatal("accepted client not registered")
	}
	if _, err := b.sessionMgr.Get(id); err == nil {
		t.Fatal("ephemeral session persisted")
	}
	mqtt5ITSubscribe(t, conn, 1, mqtt5.SubOptions{Topic: "fresh/topic"}, nil)
	mqtt5ITWrite(t, conn, &mqtt5.Disconnect{Properties: &mqtt5.Properties{}})
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, active := b.clients.Load(id); !active {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("ephemeral client did not disconnect")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := b.sessionMgr.Get(id); err == nil {
		t.Fatal("ephemeral session survived disconnect")
	}
	second, ack := mqtt5ITConnect(t, addr, id, func(c *mqtt5.Connect) { c.CleanStart = false })
	if ack.ReasonCode != 0 || ack.SessionPresent {
		t.Fatalf("fresh reconnect rejected or resumed: %#v", ack)
	}
	mqtt5ITPing(t, second)
	mqtt5ITPublish(t, second, &mqtt5.Publish{Topic: "fresh/topic", Payload: []byte("no old subscription"), QoS: 1, PacketID: 1})
	mqtt5ITQuiet(t, second)
}
