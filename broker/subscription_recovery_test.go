package broker

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/fhmq/hmq/v4/broker/packets"
	"github.com/fhmq/hmq/v4/broker/lib/sessions"
	"github.com/fhmq/hmq/v4/plugins/bridge"
)

type blockingACL struct{ started, release chan struct{} }

func (a *blockingACL) CheckConnect(string, string, string) bool { return true }
func (a *blockingACL) CheckACL(_, _, _, _, _ string) bool       { close(a.started); <-a.release; return true }

func TestClosedClientCannotRegisterQueuedSubscribe(t *testing.T) {
	b, err := NewBroker(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	a := &blockingACL{make(chan struct{}), make(chan struct{})}
	b.auth = a
	server, peer := net.Pipe()
	defer peer.Close()
	c := &client{broker: b, conn: server, typ: CLIENT, session: &sessions.Session{}}
	c.init()
	connect := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	if err := c.session.Init(connect); err != nil {
		t.Fatal(err)
	}
	sub := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	sub.MessageID, sub.Topics, sub.Qoss = 1, []string{"queued/topic"}, []byte{1}
	done := make(chan struct{})
	go func() { c.ProcessSubscribe(sub); close(done) }()
	select {
	case <-a.started:
	case <-time.After(time.Second):
		t.Fatal("ACL did not start")
	}
	c.Close()
	close(a.release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("subscribe did not finish")
	}
	filters, _, err := c.session.Topics()
	if err != nil || len(filters) != 0 {
		t.Fatalf("stale session subscriptions: %v, %v", filters, err)
	}
	var subscribers []interface{}
	var qoss []byte
	if err := b.topicsMgr.Subscribers([]byte("queued/topic"), 1, &subscribers, &qoss); err != nil || len(subscribers) != 0 {
		t.Fatalf("stale subscribers: %v, %v", subscribers, err)
	}
}

type blockingDisconnectBridge struct {
	once             sync.Once
	started, release chan struct{}
}

func (b *blockingDisconnectBridge) Publish(e *bridge.Elements) (bool, error) {
	if e.Action == bridge.Disconnect {
		b.once.Do(func() { close(b.started); <-b.release })
	}
	return false, nil
}

func TestBlockedDisconnectNotificationDoesNotHoldClientLock(t *testing.T) {
	b, err := NewBroker(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	bridge := &blockingDisconnectBridge{started: make(chan struct{}), release: make(chan struct{})}
	b.bridgeMQ = bridge
	var release sync.Once
	defer release.Do(func() { close(bridge.release) })
	connect := func() (net.Conn, <-chan error) {
		t.Helper()
		server, peer := net.Pipe()
		peer.SetDeadline(time.Now().Add(3 * time.Second))
		done := make(chan error, 1)
		go func() { done <- b.handleConnection(CLIENT, server) }()
		sendTestConnect(t, peer, "same-id", false)
		p, err := packets.ReadPacket(peer)
		if err != nil {
			peer.Close()
			t.Fatal(err)
		}
		if _, ok := p.(*packets.ConnackPacket); !ok {
			t.Fatalf("unexpected %T", p)
		}
		return peer, done
	}
	first, firstDone := connect()
	defer first.Close()
	second, secondDone := connect()
	defer second.Close()
	select {
	case <-bridge.started:
	case <-time.After(time.Second):
		t.Fatal("disconnect notification did not start")
	}
	third, thirdDone := connect()
	third.Close()
	release.Do(func() { close(bridge.release) })
	second.Close()
	first.Close()
	for _, done := range []<-chan error{firstDone, secondDone, thirdDone} {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatal("connection handler stuck")
		}
	}
}

func TestMQTTSubscriptionsRestoredAfterTakeover(t *testing.T) {
	b, err := NewBroker(&Config{Host: "127.0.0.1", Port: "0"})
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
	if addr == "" {
		t.Fatal("listener not ready")
	}
	connect := func(id string, clean, present bool) net.Conn {
		t.Helper()
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		c.SetDeadline(time.Now().Add(4 * time.Second))
		sendTestConnect(t, c, id, clean)
		p, err := packets.ReadPacket(c)
		if err != nil {
			t.Fatal(err)
		}
		ack, ok := p.(*packets.ConnackPacket)
		if !ok || ack.SessionPresent != present {
			t.Fatalf("CONNACK: %#v", p)
		}
		ping := packets.NewControlPacket(packets.Pingreq)
		if err := ping.Write(c); err != nil {
			t.Fatal(err)
		}
		if p, err := packets.ReadPacket(c); err != nil {
			t.Fatal(err)
		} else if _, ok := p.(*packets.PingrespPacket); !ok {
			t.Fatalf("PINGRESP: %T", p)
		}
		return c
	}
	old := connect("restore-sub", false, false)
	sub := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	sub.MessageID, sub.Topics, sub.Qoss = 1, []string{"restore/normal", "$share/group/restore/shared"}, []byte{1, 1}
	if err := sub.Write(old); err != nil {
		t.Fatal(err)
	}
	if p, err := packets.ReadPacket(old); err != nil {
		t.Fatal(err)
	} else if _, ok := p.(*packets.SubackPacket); !ok {
		t.Fatalf("SUBACK: %T", p)
	}
	current := connect("restore-sub", false, true)
	pub := connect("restore-pub", true, false)
	for _, topic := range []string{"restore/normal", "restore/shared"} {
		p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		p.TopicName, p.Payload = topic, []byte(topic)
		if err := p.Write(pub); err != nil {
			t.Fatal(err)
		}
		got, err := packets.ReadPacket(current)
		if err != nil {
			t.Fatal(err)
		}
		delivery, ok := got.(*packets.PublishPacket)
		if !ok || delivery.TopicName != topic {
			t.Fatalf("delivery: %#v", got)
		}
	}
}

type signalWriteConn struct {
	net.Conn
	started chan struct{}
}

func (c *signalWriteConn) Write(p []byte) (int, error) { close(c.started); return c.Conn.Write(p) }

func TestConnackTimeoutReleasesSameClientID(t *testing.T) {
	b, err := NewBroker(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	server, peer := net.Pipe()
	defer peer.Close()
	first := &signalWriteConn{Conn: server, started: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- b.handleConnection(CLIENT, first) }()
	sendTestConnect(t, peer, "blocked", false)
	select {
	case <-first.started:
	case <-time.After(time.Second):
		t.Fatal("CONNACK not started")
	}
	server2, peer2 := net.Pipe()
	defer peer2.Close()
	finished := make(chan error, 1)
	go func() { finished <- b.handleConnection(CLIENT, server2) }()
	peer2.SetDeadline(time.Now().Add(5 * time.Second))
	sendTestConnect(t, peer2, "blocked", false)
	p, err := packets.ReadPacket(peer2)
	if err != nil {
		t.Fatalf("second connection blocked: %v", err)
	}
	if _, ok := p.(*packets.ConnackPacket); !ok {
		t.Fatalf("unexpected %T", p)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected timeout")
		}
	case <-time.After(time.Second):
		t.Fatal("first handshake did not time out")
	}
	peer2.Close()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("second connection stuck")
	}
}
