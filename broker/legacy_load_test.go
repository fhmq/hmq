package broker

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/fhmq/hmq/v4/broker/packets"
)

func legacyLoadConnect(t *testing.T, addr, id string) net.Conn {
	t.Helper()
	c := mqtt5ITDial(t, addr)
	_ = c.SetDeadline(time.Now().Add(time.Minute))
	p := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	p.ProtocolName, p.ProtocolVersion, p.CleanSession, p.ClientIdentifier = "MQTT", 4, true, id
	if err := p.Write(c); err != nil {
		t.Fatal(err)
	}
	a, err := packets.ReadPacket(c)
	if ack, ok := a.(*packets.ConnackPacket); err != nil || !ok || ack.ReturnCode != packets.Accepted {
		t.Fatalf("CONNACK: %#v, %v", a, err)
	}
	return c
}

func legacyLoadSubscribe(t *testing.T, c net.Conn, topic string, qos byte) {
	t.Helper()
	p := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	p.MessageID, p.Topics, p.Qoss = 1, []string{topic}, []byte{qos}
	if err := p.Write(c); err != nil {
		t.Fatal(err)
	}
	a, err := packets.ReadPacket(c)
	if ack, ok := a.(*packets.SubackPacket); err != nil || !ok || len(ack.ReturnCodes) != 1 || ack.ReturnCodes[0] != qos {
		t.Fatalf("SUBACK: %#v, %v", a, err)
	}
}

func legacyLoadPublish(c net.Conn, p *packets.PublishPacket) error {
	if err := p.Write(c); err != nil {
		return err
	}
	a, err := packets.ReadPacket(c)
	if err != nil {
		return err
	}
	if p.Qos == 1 {
		if ack, ok := a.(*packets.PubackPacket); !ok || ack.MessageID != p.MessageID {
			return fmt.Errorf("expected PUBACK, got %#v", a)
		}
		return nil
	}
	if ack, ok := a.(*packets.PubrecPacket); !ok || ack.MessageID != p.MessageID {
		return fmt.Errorf("expected PUBREC, got %#v", a)
	}
	rel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
	rel.MessageID = p.MessageID
	if err := rel.Write(c); err != nil {
		return err
	}
	a, err = packets.ReadPacket(c)
	if ack, ok := a.(*packets.PubcompPacket); err != nil || !ok || ack.MessageID != p.MessageID {
		return fmt.Errorf("expected PUBCOMP, got %#v: %v", a, err)
	}
	return nil
}

func TestLegacyQoSLoadAndSlowConsumer(t *testing.T) {
	addr := mqtt5ITBroker(t, &Config{Worker: 1})
	const topic = "load/slow"
	slow := legacyLoadConnect(t, addr, "slow")
	legacyLoadSubscribe(t, slow, topic, 2)
	fast := legacyLoadConnect(t, addr, "publisher")
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Payload = topic, []byte("normal application traffic")
	p.Qos, p.MessageID = 1, 1
	for i := 0; i < defaultDurableWindow; i++ {
		if err := legacyLoadPublish(fast, p); err != nil {
			t.Fatal(err)
		}
		got, err := packets.ReadPacket(slow)
		if delivery, ok := got.(*packets.PublishPacket); err != nil || !ok || delivery.Qos != 1 {
			t.Fatalf("slow subscriber got %v, %v", got, err)
		}
	}
	if err := legacyLoadPublish(fast, p); err != nil {
		t.Fatalf("saturated subscriber disrupted publisher: %v", err)
	}
	_ = slow.SetReadDeadline(time.Now().Add(2 * time.Second))
	if got, err := packets.ReadPacket(slow); err == nil {
		t.Fatalf("slow subscriber was not disconnected: %#v", got)
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatal("slow subscriber remained connected")
	}

	const messages = 30000
	for _, qos := range []byte{1, 2} {
		t.Run(fmt.Sprint(qos), func(t *testing.T) {
			pub := legacyLoadConnect(t, addr, fmt.Sprintf("load-%d", qos))
			probe := legacyLoadConnect(t, addr, fmt.Sprintf("probe-%d", qos))
			done := make(chan error, 1)
			go func() {
				publication := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
				publication.TopicName, publication.Payload, publication.Qos = "load/healthy", []byte("ok"), qos
				for i := 1; i <= messages; i++ {
					publication.MessageID = uint16(i)
					if err := legacyLoadPublish(pub, publication); err != nil {
						done <- fmt.Errorf("publication %d: %w", i, err)
						return
					}
				}
				done <- nil
			}()
			for {
				_ = probe.SetDeadline(time.Now().Add(2 * time.Second))
				if err := packets.NewControlPacket(packets.Pingreq).Write(probe); err != nil {
					t.Fatal(err)
				}
				p, err := packets.ReadPacket(probe)
				if _, ok := p.(*packets.PingrespPacket); err != nil || !ok {
					t.Fatalf("probe unresponsive: %#v, %v", p, err)
				}
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
					return
				case <-time.After(50 * time.Millisecond):
				}
			}
		})
	}
}

func TestBlockedSubscriberDoesNotBlockUnrelatedConnection(t *testing.T) {
	b, err := NewBroker(&Config{Worker: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	attach := func(server, peer net.Conn, id string) {
		t.Helper()
		t.Cleanup(func() { _ = peer.Close() })
		_ = peer.SetDeadline(time.Now().Add(5 * time.Second))
		go b.handleConnection(CLIENT, server)
		sendTestConnect(t, peer, id, true)
		if a, err := packets.ReadPacket(peer); err != nil {
			t.Fatalf("connection %s: %#v, %v", id, a, err)
		}
	}
	slowServer, slow := net.Pipe()
	observed := &publishSignalConn{Conn: slowServer, started: make(chan struct{})}
	attach(observed, slow, "stalled")
	// Cleanup must unblock the local connection handlers before Broker.Close.
	defer slow.Close()
	legacyLoadSubscribe(t, slow, "load/blocked", 1)
	pubServer, pub := net.Pipe()
	attach(pubServer, pub, "blocked-publisher")
	defer pub.Close()
	probeServer, probe := net.Pipe()
	attach(probeServer, probe, "independent-client")
	defer probe.Close()

	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Qos, p.MessageID = "load/blocked", 1, 7
	done := make(chan error, 1)
	go func() { done <- legacyLoadPublish(pub, p) }()
	select {
	case <-observed.started:
	case <-time.After(time.Second):
		t.Fatal("subscriber write did not start")
	}
	_ = probe.SetDeadline(time.Now().Add(time.Second))
	if err := packets.NewControlPacket(packets.Pingreq).Write(probe); err != nil {
		t.Fatal(err)
	}
	response, err := packets.ReadPacket(probe)
	if _, ok := response.(*packets.PingrespPacket); err != nil || !ok {
		t.Fatalf("unrelated connection blocked: %#v, %v", response, err)
	}
	if err := <-done; err != nil {
		t.Fatalf("subscriber timeout disrupted publisher: %v", err)
	}
}
