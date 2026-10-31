package broker

import (
	"errors"
	"github.com/fhmq/hmq/v4/broker/packets"
	"github.com/fhmq/hmq/v4/broker/lib/sessions"
	"github.com/fhmq/hmq/v4/broker/sessionstore"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func subscribeLog(t *testing.T, c net.Conn) {
	t.Helper()
	s := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	s.MessageID, s.Topics, s.Qoss = 1, []string{"race/+"}, []byte{1}
	if err := s.Write(c); err != nil {
		t.Fatal(err)
	}
	if _, err := packets.ReadPacket(c); err != nil {
		t.Fatal(err)
	}
}
func waitLogDone(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("connection stuck")
	}
}

func TestSessionLogDisconnectInterleavings(t *testing.T) {
	for _, stage := range []string{"online", "persisted"} {
		t.Run(stage, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sessions.log")
			b, err := NewBroker(&Config{ExperimentalSessionLogPath: path})
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			sub, done := logMQTTConnect(t, b, "subscriber", false)
			subscribeLog(t, sub)
			pub, pubDone := logMQTTConnect(t, b, "publisher", false)
			entered, release := make(chan struct{}), make(chan struct{})
			b.durableRouteHook = func(id, at string) {
				if id == "subscriber" && at == stage {
					close(entered)
					<-release
				}
			}
			p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			p.TopicName, p.Payload, p.Qos, p.MessageID = "race/a", []byte(stage), 1, 7
			if err := p.Write(pub); err != nil {
				t.Fatal(err)
			}
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("barrier not reached")
			}
			sub.Close()
			waitLogDone(t, done)
			close(release)
			a, err := packets.ReadPacket(pub)
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := a.(*packets.PubackPacket); !ok {
				t.Fatalf("publisher ACK: %T", a)
			}
			pub.Close()
			waitLogDone(t, pubDone)
			s, _, err := b.sessionLog.Get("subscriber")
			if err != nil {
				t.Fatal(err)
			}
			if len(s.Queue)+len(s.Inflight) != 1 {
				t.Fatalf("lost or duplicated: %#v", s)
			}
			b.Close()
			b, err = NewBroker(&Config{ExperimentalSessionLogPath: path})
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			sub, done = logMQTTConnect(t, b, "subscriber", true)
			a, err = packets.ReadPacket(sub)
			if err != nil {
				t.Fatal(err)
			}
			delivery := a.(*packets.PublishPacket)
			if string(delivery.Payload) != stage || !delivery.Dup {
				t.Fatalf("replay: %#v", delivery)
			}
			sub.Close()
			waitLogDone(t, done)
		})
	}
}

func TestSessionLogNoMatchDoesNotWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.log")
	b, err := NewBroker(&Config{ExperimentalSessionLogPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	c, done := logMQTTConnect(t, b, "sub", false)
	subscribeLog(t, c)
	c.Close()
	waitLogDone(t, done)
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Qos = "unmatched", 1
	if err := b.publishLocal(p); err != nil {
		t.Fatal(err)
	}
	after, _ := os.Stat(path)
	if before.Size() != after.Size() {
		t.Fatal("unmatched publish wrote WAL")
	}
}

func TestSessionLogEpochRejectsStaleOwner(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(map[bool]string{false: "resume", true: "clean"}[clean], func(t *testing.T) {
			b, err := NewBroker(&Config{ExperimentalSessionLogPath: filepath.Join(t.TempDir(), "sessions.log")})
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			peer, done := logMQTTConnect(t, b, "owner", false)
			subscribeLog(t, peer)
			value, _ := b.clients.Load("owner")
			old := value.(*client)
			server, current := net.Pipe()
			current.SetDeadline(time.Now().Add(5 * time.Second))
			next := make(chan error, 1)
			go func() { next <- b.handleConnection(CLIENT, server) }()
			sendTestConnect(t, current, "owner", clean)
			if _, err := packets.ReadPacket(current); err != nil {
				t.Fatal(err)
			}
			// PING fences completion of takeover before simulating a paused old write.
			if err := packets.NewControlPacket(packets.Pingreq).Write(current); err != nil {
				t.Fatal(err)
			}
			if _, err := packets.ReadPacket(current); err != nil {
				t.Fatal(err)
			}
			p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			p.TopicName, p.Qos, p.MessageID = "race/a", 1, 9
			if !clean {
				sent := make(chan error, 1)
				go func() { sent <- b.publishLocal(p) }()
				a, err := packets.ReadPacket(current)
				if err != nil {
					t.Fatal(err)
				}
				delivery := a.(*packets.PublishPacket)
				if err := <-sent; err != nil {
					t.Fatal(err)
				}
				ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
				ack.MessageID = delivery.MessageID
				if err := ack.Write(current); err != nil {
					t.Fatal(err)
				}
				if err := packets.NewControlPacket(packets.Pingreq).Write(current); err != nil {
					t.Fatal(err)
				}
				if _, err := packets.ReadPacket(current); err != nil {
					t.Fatal(err)
				}
				p.MessageID = delivery.MessageID
			}
			if err := old.persistOutbound(p.MessageID, &sessions.OutboundMessage{Packet: p}); !errors.Is(err, sessionstore.ErrConflict) {
				t.Fatalf("stale write: %v", err)
			}
			if err := publishDelivery(&subscription{client: old}, p, 1, true); !errors.Is(err, net.ErrClosed) {
				t.Fatalf("stale retained write: %v", err)
			}
			s, _, err := b.sessionLog.Get("owner")
			if err != nil {
				t.Fatal(err)
			}
			if clean && s != nil || s != nil && len(s.Inflight) != 0 {
				t.Fatalf("stale owner resurrected state: %#v", s)
			}
			if clean {
				current.Close()
				waitLogDone(t, next)
				fresh, freshDone := logMQTTConnect(t, b, "owner", false)
				if err := packets.NewControlPacket(packets.Pingreq).Write(fresh); err != nil {
					t.Fatal(err)
				}
				if _, err := packets.ReadPacket(fresh); err != nil {
					t.Fatal(err)
				}
				if err := old.persistOutbound(9, &sessions.OutboundMessage{Packet: p}); !errors.Is(err, sessionstore.ErrConflict) {
					t.Fatalf("epoch ABA after clean: %v", err)
				}
				fresh.Close()
				waitLogDone(t, freshDone)
				peer.Close()
				waitLogDone(t, done)
				return
			}
			peer.Close()
			waitLogDone(t, done)
			current.Close()
			waitLogDone(t, next)
		})
	}
}

func TestSessionLogStorageFailureBeforeConnack(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(map[bool]string{false: "create", true: "delete"}[clean], func(t *testing.T) {
			b, err := NewBroker(&Config{ExperimentalSessionLogPath: filepath.Join(t.TempDir(), "sessions.log")})
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			old, oldDone := logMQTTConnect(t, b, "old", false)
			subscribeLog(t, old)
			value, _ := b.clients.Load("old")
			if err := b.sessionLog.Close(); err != nil {
				t.Fatal(err)
			}
			server, peer := net.Pipe()
			peer.SetDeadline(time.Now().Add(3 * time.Second))
			defer peer.Close()
			done := make(chan error, 1)
			go func() { done <- b.handleConnection(CLIENT, server) }()
			id := "new"
			if clean {
				id = "old"
			}
			sendTestConnect(t, peer, id, clean)
			if p, err := packets.ReadPacket(peer); err == nil {
				t.Fatalf("storage failure confirmed: %#v", p)
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("missing failure")
				}
			case <-time.After(4 * time.Second):
				t.Fatal("handshake stuck")
			}
			if current, _ := b.clients.Load("old"); current != value || value.(*client).isDisconnected() {
				t.Fatal("failed storage detached old owner")
			}
			old.Close()
			waitLogDone(t, oldDone)
		})
	}
}

func TestSessionLogFailedConnackRollsBackEpoch(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(map[bool]string{false: "resume", true: "clean"}[clean], func(t *testing.T) {
			b, err := NewBroker(&Config{ExperimentalSessionLogPath: filepath.Join(t.TempDir(), "sessions.log")})
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			old, oldDone := logMQTTConnect(t, b, "owner", false)
			subscribeLog(t, old)
			value, _ := b.clients.Load("owner")
			owner := value.(*client)
			server, peer := net.Pipe()
			defer peer.Close()
			blocked := &blockedConnackConn{Conn: server, started: make(chan struct{}), release: make(chan struct{})}
			done := make(chan error, 1)
			go func() { done <- b.handleConnection(CLIENT, blocked) }()
			sendTestConnect(t, peer, "owner", clean)
			select {
			case <-blocked.started:
			case <-time.After(3 * time.Second):
				t.Fatal("CONNACK not attempted")
			}
			close(blocked.release)
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("expected CONNACK failure")
				}
			case <-time.After(3 * time.Second):
				t.Fatal("failed handshake stuck")
			}
			current, _ := b.clients.Load("owner")
			if current != owner || owner.isDisconnected() {
				t.Fatal("old owner changed")
			}
			state, _, err := b.sessionLog.Get("owner")
			if err != nil || state == nil || state.Epoch != owner.durableEpoch || len(state.Subscriptions) != 1 {
				t.Fatalf("rollback: %#v %v", state, err)
			}
			p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			p.TopicName, p.Qos, p.MessageID = "race/a", 1, 8
			if err := owner.persistOutbound(8, &sessions.OutboundMessage{Packet: p}); err != nil {
				t.Fatalf("old owner cannot commit: %v", err)
			}
			old.Close()
			waitLogDone(t, oldDone)
		})
	}
}
