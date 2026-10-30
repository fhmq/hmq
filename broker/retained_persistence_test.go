package broker

import (
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/v3/broker/lib/topics"
	"github.com/fhmq/hmq/v3/broker/storage"
)

type blockingFailureRetain struct {
	topics.TopicsProvider
	entered chan struct{}
	release chan struct{}
}

func (p *blockingFailureRetain) Retain(*packets.PublishPacket) error {
	close(p.entered)
	<-p.release
	return errors.New("retain failed")
}

func TestRetainFailureConcurrentClose(t *testing.T) {
	p := &blockingFailureRetain{TopicsProvider: topics.NewMemProvider(), entered: make(chan struct{}), release: make(chan struct{})}
	const name = "blocking-retain-close-test"
	topics.Register(name, p)
	defer topics.Unregister(name)
	mgr, err := topics.NewManager(name)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewBroker(&Config{Host: "127.0.0.1", Port: "0"})
	if err != nil {
		t.Fatal(err)
	}
	b.topicsMgr = mgr
	b.Start()
	defer b.Close()
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
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	connect := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	connect.ProtocolName, connect.ProtocolVersion, connect.CleanSession, connect.ClientIdentifier = "MQTT", 4, true, "close-race"
	if err := connect.Write(conn); err != nil {
		t.Fatal(err)
	}
	if _, err := packets.ReadPacket(conn); err != nil {
		t.Fatal(err)
	}
	pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	pub.TopicName, pub.Qos, pub.MessageID, pub.Retain, pub.Payload = "race/topic", 1, 1, true, []byte("value")
	if err := pub.Write(conn); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.entered:
	case <-time.After(3 * time.Second):
		close(p.release)
		t.Fatal("retain not called")
	}
	v, ok := b.clients.Load("close-race")
	if !ok {
		close(p.release)
		t.Fatal("client not found")
	}
	v.(*client).Close()
	close(p.release)
	if packet, err := packets.ReadPacket(conn); err == nil {
		t.Fatalf("unexpected packet %T", packet)
	}
}

func TestRetainedPersistenceRestartMQTT(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retained.log")
	start := func() (*Broker, string) {
		b, err := NewBroker(&Config{Host: "127.0.0.1", Port: "0", RetainedLogPath: path})
		if err != nil {
			t.Fatal(err)
		}
		b.Start()
		for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
			b.lifecycleMu.Lock()
			var addr string
			for l := range b.listeners {
				addr = l.Addr().String()
			}
			b.lifecycleMu.Unlock()
			if addr != "" {
				return b, addr
			}
			time.Sleep(time.Millisecond)
		}
		b.Close()
		t.Fatal("listener did not start")
		return nil, ""
	}
	connect := func(addr, id string) mqtt.Client {
		c := mqtt.NewClient(mqtt.NewClientOptions().AddBroker("tcp://" + addr).SetClientID(id).SetAutoReconnect(false))
		token := c.Connect()
		if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
			t.Fatalf("connect: %v", token.Error())
		}
		return c
	}
	b, addr := start()
	if _, err := NewBroker(&Config{RetainedLogPath: path}); err == nil {
		t.Fatal("second open succeeded")
	}
	c := connect(addr, "persist-publisher")
	token := c.Publish("persist/topic", 1, true, "saved")
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		t.Fatalf("publish: %v", token.Error())
	}
	c.Disconnect(100)
	b.Close()
	b, addr = start()
	defer b.Close()
	c = connect(addr, "persist-reader")
	defer c.Disconnect(100)
	got := make(chan string, 1)
	token = c.Subscribe("persist/#", 1, func(_ mqtt.Client, m mqtt.Message) {
		if m.Retained() {
			got <- string(m.Payload())
		}
	})
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		t.Fatalf("subscribe: %v", token.Error())
	}
	select {
	case value := <-got:
		if value != "saved" {
			t.Fatalf("got %q", value)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("retained message missing")
	}
}

func TestRetainedStorageFailureNoMQTTAck(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		t.Run(string(rune('0'+qos)), func(t *testing.T) {
			b, err := NewBroker(&Config{Host: "127.0.0.1", Port: "0", RetainedLogPath: filepath.Join(t.TempDir(), "retained.log")})
			if err != nil {
				t.Fatal(err)
			}
			b.Start()
			defer b.Close()
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
			conn, err := net.DialTimeout("tcp", addr, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			connect := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
			connect.ProtocolName, connect.ProtocolVersion, connect.CleanSession, connect.ClientIdentifier = "MQTT", 4, true, "failure-test"
			if err := connect.Write(conn); err != nil {
				t.Fatal(err)
			}
			_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
			if p, err := packets.ReadPacket(conn); err != nil {
				t.Fatal(err)
			} else if _, ok := p.(*packets.ConnackPacket); !ok {
				t.Fatalf("unexpected %T", p)
			}
			v, ok := b.clients.Load("failure-test")
			if !ok {
				t.Fatal("client not registered")
			}
			client := v.(*client)
			pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			pub.TopicName, pub.Qos, pub.MessageID, pub.Retain, pub.Payload = "failure/topic", qos, 1, true, make([]byte, storage.MaxRecordSize)
			if err := pub.Write(conn); err != nil {
				t.Fatal(err)
			}
			if p, err := packets.ReadPacket(conn); err == nil {
				t.Fatalf("unexpected success packet %T", p)
			} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
				t.Fatalf("connection not closed: %v", err)
			}
			if qos == 2 {
				client.publishMu.Lock()
				client.awaitingRelMu.RLock()
				_, exists := client.awaitingRel[pub.MessageID]
				client.awaitingRelMu.RUnlock()
				client.publishMu.Unlock()
				if exists {
					t.Fatal("failed publish remains registered as awaiting PUBREL")
				}
				retry, err := net.DialTimeout("tcp", addr, time.Second)
				if err != nil {
					t.Fatal(err)
				}
				defer retry.Close()
				_ = retry.SetDeadline(time.Now().Add(10 * time.Second))
				if err := connect.Write(retry); err != nil {
					t.Fatal(err)
				}
				if _, err := packets.ReadPacket(retry); err != nil {
					t.Fatal(err)
				}
				pub.Dup = true
				if err := pub.Write(retry); err != nil {
					t.Fatal(err)
				}
				if p, err := packets.ReadPacket(retry); err == nil {
					t.Fatalf("retry received unexpected success packet %T", p)
				} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
					t.Fatalf("retry connection not closed: %v", err)
				}
			}
		})
	}
}

func TestRetainedPersistenceInvalidPath(t *testing.T) {
	for _, path := range []string{"   ", filepath.Join(t.TempDir(), "missing", "log")} {
		if b, err := NewBroker(&Config{RetainedLogPath: path}); err == nil {
			b.Close()
			t.Fatalf("accepted %q", path)
		}
	}
}

func TestRetainedReadFailureNoSubscribeAck(t *testing.T) {
	b, err := NewBroker(&Config{Host: "127.0.0.1", Port: "0", RetainedLogPath: filepath.Join(t.TempDir(), "retained.log")})
	if err != nil {
		t.Fatal(err)
	}
	b.Start()
	defer b.Close()
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Payload = "failure/topic", make([]byte, storage.MaxRecordSize)
	if err := b.topicsMgr.Retain(p); err == nil {
		t.Fatal("expected storage failure")
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
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	connect := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	connect.ProtocolName, connect.ProtocolVersion, connect.CleanSession, connect.ClientIdentifier = "MQTT", 4, true, "read-failure"
	if err := connect.Write(conn); err != nil {
		t.Fatal(err)
	}
	if _, err := packets.ReadPacket(conn); err != nil {
		t.Fatal(err)
	}
	sub := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	sub.MessageID, sub.Topics, sub.Qoss = 1, []string{"failure/#"}, []byte{1}
	if err := sub.Write(conn); err != nil {
		t.Fatal(err)
	}
	if p, err := packets.ReadPacket(conn); err == nil {
		t.Fatalf("unexpected packet %T", p)
	} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
		t.Fatalf("connection not closed: %v", err)
	}
}
