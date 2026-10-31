package broker

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/fhmq/hmq/v4/broker/packets"
)

func TestMQTTForwardFailureDoesNotAcknowledge(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		t.Run(string(rune('0'+qos)), func(t *testing.T) {
			b, err := NewBroker(&Config{Host: "127.0.0.1", Port: "0"})
			if err != nil {
				t.Fatal(err)
			}
			called := make(chan struct{}, 1)
			b.SetPublishForwarder(func(p *packets.PublishPacket) error {
				if p.TopicName != "forward/test" {
					return nil
				}
				called <- struct{}{}
				return errors.New("forward failed")
			})
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
			conn, err := net.DialTimeout("tcp", addr, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
			c := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
			c.ProtocolName, c.ProtocolVersion, c.CleanSession, c.ClientIdentifier = "MQTT", 4, true, "forward-failure"
			if err := c.Write(conn); err != nil {
				t.Fatal(err)
			}
			if _, err := packets.ReadPacket(conn); err != nil {
				t.Fatal(err)
			}
			p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			p.TopicName, p.Qos, p.MessageID, p.Payload = "forward/test", qos, 42, []byte("hello")
			if err := p.Write(conn); err != nil {
				t.Fatal(err)
			}
			ack, err := packets.ReadPacket(conn)
			if err == nil {
				t.Fatalf("unexpected packet after forward failure: %T", ack)
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatalf("connection not closed: %v", err)
			}
			select {
			case <-called:
			default:
				t.Fatal("forwarder not called")
			}
		})
	}
}
