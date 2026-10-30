package broker

import (
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/v3/plugins/bridge"
)

type consumingBridge struct{}

func (consumingBridge) Publish(*bridge.Elements) (bool, error) { return true, nil }

func TestMQTTBridgeConsumedQoS1Acknowledged(t *testing.T) {
	b, err := NewBroker(&Config{Host: "127.0.0.1", Port: "0"})
	if err != nil {
		t.Fatal(err)
	}
	b.bridgeMQ = consumingBridge{}
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
	c.ProtocolName, c.ProtocolVersion, c.CleanSession, c.ClientIdentifier = "MQTT", 4, true, "bridge-ack"
	if err := c.Write(conn); err != nil {
		t.Fatal(err)
	}
	if p, err := packets.ReadPacket(conn); err != nil {
		t.Fatal(err)
	} else if _, ok := p.(*packets.ConnackPacket); !ok {
		t.Fatalf("CONNACK: %T", p)
	}
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Qos, p.MessageID, p.Payload = "bridge/test", 1, 42, []byte("hello")
	if err := p.Write(conn); err != nil {
		t.Fatal(err)
	}
	ack, err := packets.ReadPacket(conn)
	if err != nil {
		t.Fatal(err)
	}
	if a, ok := ack.(*packets.PubackPacket); !ok || a.MessageID != 42 {
		t.Fatalf("PUBACK: %#v", ack)
	}
}
