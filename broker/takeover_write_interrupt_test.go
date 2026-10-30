package broker

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

type failConnackConn struct{ net.Conn }

func (c failConnackConn) Write([]byte) (int, error) { return 0, errors.New("injected CONNACK failure") }

type publishSignalConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func TestPublisherSurvivesInterruptedTakeoverDelivery(t *testing.T) {
	for _, qos := range []byte{0, 1} {
		for _, failConnack := range []bool{false, true} {
			t.Run(fmt.Sprintf("qos%d/failed_connack_%t", qos, failConnack), func(t *testing.T) {
				b, err := NewBroker(&Config{})
				if err != nil {
					t.Fatal(err)
				}
				defer b.Close()
				server, subscriber := net.Pipe()
				old := &publishSignalConn{Conn: server, started: make(chan struct{})}
				defer subscriber.Close()
				_ = subscriber.SetDeadline(time.Now().Add(5 * time.Second))
				go b.handleConnection(CLIENT, old)
				sendTestConnect(t, subscriber, "interrupt-sub", false)
				if _, err := packets.ReadPacket(subscriber); err != nil {
					t.Fatal(err)
				}
				sub := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
				sub.MessageID, sub.Topics, sub.Qoss = 1, []string{"interrupt/topic"}, []byte{1}
				if err := sub.Write(subscriber); err != nil {
					t.Fatal(err)
				}
				if _, err := packets.ReadPacket(subscriber); err != nil {
					t.Fatal(err)
				}
				pubServer, publisher := net.Pipe()
				defer publisher.Close()
				_ = publisher.SetDeadline(time.Now().Add(5 * time.Second))
				go b.handleConnection(CLIENT, pubServer)
				sendTestConnect(t, publisher, "interrupt-pub", true)
				if _, err := packets.ReadPacket(publisher); err != nil {
					t.Fatal(err)
				}
				p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
				p.TopicName, p.Payload, p.Qos, p.MessageID = "interrupt/topic", []byte("message"), qos, 9
				if err := p.Write(publisher); err != nil {
					t.Fatal(err)
				}
				select {
				case <-old.started:
				case <-time.After(time.Second):
					t.Fatal("delivery did not start")
				}
				newServer, current := net.Pipe()
				defer current.Close()
				_ = current.SetDeadline(time.Now().Add(5 * time.Second))
				takeoverDone := make(chan error, 1)
				go func() {
					var conn net.Conn = newServer
					if failConnack {
						conn = failConnackConn{newServer}
					}
					takeoverDone <- b.handleConnection(CLIENT, conn)
				}()
				sendTestConnect(t, current, "interrupt-sub", false)
				if failConnack {
					select {
					case err := <-takeoverDone:
						if err == nil {
							t.Fatal("expected failed CONNACK")
						}
					case <-time.After(time.Second):
						t.Fatal("failed CONNACK blocked")
					}
				} else {
					ack, err := packets.ReadPacket(current)
					if err != nil {
						t.Fatal(err)
					}
					if a, ok := ack.(*packets.ConnackPacket); !ok || !a.SessionPresent {
						t.Fatalf("CONNACK = %#v", ack)
					}
				}
				if qos == 1 {
					ack, err := packets.ReadPacket(publisher)
					if err != nil {
						t.Fatalf("publisher PUBACK: %v", err)
					}
					if _, ok := ack.(*packets.PubackPacket); !ok {
						t.Fatalf("expected PUBACK: %#v", ack)
					}
				}
				ping := packets.NewControlPacket(packets.Pingreq)
				if err := ping.Write(publisher); err != nil {
					t.Fatalf("publisher disconnected: %v", err)
				}
				if packet, err := packets.ReadPacket(publisher); err != nil {
					t.Fatalf("publisher PINGRESP: %v", err)
				} else if _, ok := packet.(*packets.PingrespPacket); !ok {
					t.Fatalf("PINGRESP = %#v", packet)
				}
				if qos == 1 {
					recovery := current
					if failConnack {
						recoveryServer, recoveryPeer := net.Pipe()
						defer recoveryPeer.Close()
						_ = recoveryPeer.SetDeadline(time.Now().Add(5 * time.Second))
						go b.handleConnection(CLIENT, recoveryServer)
						sendTestConnect(t, recoveryPeer, "interrupt-sub", false)
						ack, err := packets.ReadPacket(recoveryPeer)
						if err != nil {
							t.Fatal(err)
						}
						if a, ok := ack.(*packets.ConnackPacket); !ok || !a.SessionPresent {
							t.Fatalf("recovery CONNACK = %#v", ack)
						}
						recovery = recoveryPeer
					}
					packet, err := packets.ReadPacket(recovery)
					if err != nil {
						t.Fatalf("recovered delivery: %v", err)
					}
					message, ok := packet.(*packets.PublishPacket)
					if !ok || string(message.Payload) != "message" || !message.Dup {
						t.Fatalf("recovered delivery = %#v", packet)
					}
				}
			})
		}
	}
}

func (c *publishSignalConn) Write(p []byte) (int, error) {
	if len(p) > 0 && p[0]>>4 == packets.Publish {
		c.once.Do(func() { close(c.started) })
	}
	return c.Conn.Write(p)
}

func TestTakeoverInterruptsBlockedDelivery(t *testing.T) {
	b, err := NewBroker(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	server, peer := net.Pipe()
	old := &publishSignalConn{Conn: server, started: make(chan struct{})}
	defer peer.Close()
	_ = peer.SetDeadline(time.Now().Add(5 * time.Second))
	go b.handleConnection(CLIENT, old)
	sendTestConnect(t, peer, "slow-takeover", false)
	if _, err := packets.ReadPacket(peer); err != nil {
		t.Fatal(err)
	}
	sub := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	sub.MessageID, sub.Topics, sub.Qoss = 1, []string{"slow/topic"}, []byte{0}
	if err := sub.Write(peer); err != nil {
		t.Fatal(err)
	}
	if _, err := packets.ReadPacket(peer); err != nil {
		t.Fatal(err)
	}
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName = "slow/topic"
	done := make(chan error, 1)
	go func() { done <- b.publishLocal(p) }()
	select {
	case <-old.started:
	case <-time.After(time.Second):
		t.Fatal("delivery did not start")
	}
	newServer, current := net.Pipe()
	defer current.Close()
	_ = current.SetDeadline(time.Now().Add(time.Second))
	go b.handleConnection(CLIENT, newServer)
	sendTestConnect(t, current, "slow-takeover", false)
	if _, err := packets.ReadPacket(current); err != nil {
		t.Fatalf("takeover blocked: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("takeover interruption escaped publish: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery did not unblock")
	}
}
