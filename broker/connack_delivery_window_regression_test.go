package broker

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

type windowConn struct {
	net.Conn
	writeDone, writeRelease    chan struct{}
	closeStarted, closeRelease chan struct{}
	writeOnce, closeOnce       sync.Once
}

func (c *windowConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if c.writeDone != nil {
		c.writeOnce.Do(func() { close(c.writeDone); <-c.writeRelease })
	}
	return n, err
}

func (c *windowConn) Close() error {
	if c.closeStarted != nil {
		c.closeOnce.Do(func() { close(c.closeStarted); <-c.closeRelease })
	}
	return c.Conn.Close()
}

func TestConnackTakeoverPublishWindow(t *testing.T) {
	b, err := NewBroker(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()

	oldServer, old := net.Pipe()
	oldWrapped := &windowConn{Conn: oldServer, closeStarted: make(chan struct{}), closeRelease: make(chan struct{})}
	newServer, current := net.Pipe()
	newWrapped := &windowConn{Conn: newServer, writeDone: make(chan struct{}), writeRelease: make(chan struct{})}
	pubServer, pub := net.Pipe()
	var releaseOld, releaseNew sync.Once
	defer func() {
		releaseNew.Do(func() { close(newWrapped.writeRelease) })
		releaseOld.Do(func() { close(oldWrapped.closeRelease) })
		old.Close()
		current.Close()
		pub.Close()
	}()
	for _, c := range []net.Conn{old, current, pub} {
		_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	}
	go b.handleConnection(CLIENT, oldWrapped)
	sendTestConnect(t, old, "window-sub", false)
	if _, err := packets.ReadPacket(old); err != nil {
		t.Fatal(err)
	}
	sub := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	sub.MessageID, sub.Topics, sub.Qoss = 1, []string{"window/topic"}, []byte{1}
	if err := sub.Write(old); err != nil {
		t.Fatal(err)
	}
	if p, err := packets.ReadPacket(old); err != nil {
		t.Fatal(err)
	} else if _, ok := p.(*packets.SubackPacket); !ok {
		t.Fatalf("expected SUBACK, got %T", p)
	}
	go b.handleConnection(CLIENT, pubServer)
	sendTestConnect(t, pub, "window-pub", true)
	if _, err := packets.ReadPacket(pub); err != nil {
		t.Fatal(err)
	}
	go b.handleConnection(CLIENT, newWrapped)
	sendTestConnect(t, current, "window-sub", false)
	ack, err := packets.ReadPacket(current)
	if err != nil {
		t.Fatal(err)
	}
	if a, ok := ack.(*packets.ConnackPacket); !ok || !a.SessionPresent {
		t.Fatalf("unexpected CONNACK: %#v", ack)
	}
	select {
	case <-newWrapped.writeDone:
	case <-time.After(time.Second):
		t.Fatal("CONNACK write did not pause")
	}
	releaseNew.Do(func() { close(newWrapped.writeRelease) })
	select {
	case <-oldWrapped.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("old connection cleanup did not pause")
	}
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Payload, p.Qos, p.MessageID = "window/topic", []byte("during takeover"), 1, 9
	if err := p.Write(pub); err != nil {
		t.Fatal(err)
	}
	type readResult struct {
		packet packets.ControlPacket
		err    error
	}
	delivery := make(chan readResult, 1)
	go func() {
		packet, err := packets.ReadPacket(current)
		delivery <- readResult{packet, err}
	}()
	response, pubErr := packets.ReadPacket(pub)
	releaseOld.Do(func() { close(oldWrapped.closeRelease) })
	if pubErr != nil {
		t.Fatalf("publish lost during takeover: publisher disconnected before PUBACK: %v", pubErr)
	}
	if _, ok := response.(*packets.PubackPacket); !ok {
		t.Fatalf("expected PUBACK, got %T", response)
	}
	result := <-delivery
	got, err := result.packet, result.err
	if err != nil {
		t.Fatalf("publication missing after CONNACK: %v", err)
	}
	message, ok := got.(*packets.PublishPacket)
	if !ok || string(message.Payload) != "during takeover" {
		t.Fatalf("unexpected delivery: %#v", got)
	}
}
