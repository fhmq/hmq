package broker

import (
	"fmt"
	"net"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func TestMQTTEndToEnd(t *testing.T) {
	b, err := NewBroker(&Config{Host: "127.0.0.1", Port: "0"})
	if err != nil {
		t.Fatal(err)
	}
	b.Start()
	t.Cleanup(b.Close)
	var addr string
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
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
	connect := func(id string) mqtt.Client {
		opts := mqtt.NewClientOptions().AddBroker("tcp://" + addr).SetClientID(id).SetAutoReconnect(false).SetConnectTimeout(2 * time.Second)
		c := mqtt.NewClient(opts)
		token := c.Connect()
		if !token.WaitTimeout(3 * time.Second) {
			t.Fatal("connect timed out")
		}
		if err := token.Error(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Disconnect(100) })
		return c
	}
	sub := connect("e2e-subscriber")
	pub := connect("e2e-publisher")
	messages := make(chan string, 1)
	token := sub.Subscribe("e2e/topic", 1, func(_ mqtt.Client, m mqtt.Message) { messages <- fmt.Sprintf("%s:%s", m.Topic(), m.Payload()) })
	if !token.WaitTimeout(3 * time.Second) {
		t.Fatal("subscribe timed out")
	}
	if err := token.Error(); err != nil {
		t.Fatal(err)
	}
	token = pub.Publish("e2e/topic", 1, false, "hello")
	if !token.WaitTimeout(3 * time.Second) {
		t.Fatal("publish timed out")
	}
	if err := token.Error(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-messages:
		if got != "e2e/topic:hello" {
			t.Fatalf("unexpected message %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("message not delivered")
	}
	pub.Disconnect(100)
	done := make(chan struct{})
	go func() { b.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("broker close timed out")
	}
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err == nil {
		conn.Close()
		t.Fatal("listener still accepts connections after close")
	}
}
