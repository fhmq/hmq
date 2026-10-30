package broker

import (
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

func TestMQTTLocalPublishFailureDisconnects(t *testing.T) {
	for _, qos := range []byte{0, 1, 2} {
		t.Run(fmt.Sprint(qos), func(t *testing.T) {
			b, err := NewBroker(&Config{Host: "127.0.0.1", Port: "0"})
			if err != nil {
				t.Fatal(err)
			}
			b.Start()
			t.Cleanup(b.Close)
			// A current subscriber with a broken connection fails delivery.
			failed := &client{status: Connected, info: info{clientID: "failing-subscriber"}, inflight: make(map[uint16]*inflightElem)}
			sub := &subscription{client: failed, topic: "failure/test", qos: 2}
			failed.subMap = map[string]*subscription{"failure/test": sub}
			b.clients.Store(failed.info.clientID, failed)
			t.Cleanup(func() { b.clients.Delete(failed.info.clientID) })
			_, err = b.topicsMgr.Subscribe([]byte("failure/test"), 2, sub)
			if err != nil {
				t.Fatal(err)
			}
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
			_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
			connect := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
			connect.ProtocolName, connect.ProtocolVersion, connect.CleanSession, connect.ClientIdentifier = "MQTT", 4, true, "local-failure"
			if err := connect.Write(conn); err != nil {
				t.Fatal(err)
			}
			p, err := packets.ReadPacket(conn)
			if err != nil {
				t.Fatal(err)
			}
			if ack, ok := p.(*packets.ConnackPacket); !ok || ack.ReturnCode != 0 {
				t.Fatalf("CONNACK: %#v", p)
			}
			var c *client
			for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); {
				if v, ok := b.clients.Load("local-failure"); ok {
					c = v.(*client)
					break
				}
				time.Sleep(time.Millisecond)
			}
			if c == nil {
				t.Fatal("client not registered")
			}
			pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			pub.TopicName, pub.Qos, pub.MessageID, pub.Payload = "failure/test", qos, 42, []byte("message")
			if err := pub.Write(conn); err != nil {
				t.Fatal(err)
			}
			p, err = packets.ReadPacket(conn)
			if !errors.Is(err, io.EOF) {
				t.Fatalf("expected disconnect without acknowledgment, got %#v, %v", p, err)
			}
			// The handler can finish its deferred rollback just after the socket closes.
			c.publishMu.Lock()
			c.awaitingRelMu.Lock()
			_, exists := c.awaitingRel[42]
			c.awaitingRelMu.Unlock()
			c.publishMu.Unlock()
			if exists {
				t.Fatal("QoS2 packet ID was not rolled back")
			}
		})
	}
}
