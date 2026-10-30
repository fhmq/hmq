package broker

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/v3/broker/lib/sessions"
)

type controlledFailureConn struct {
	net.Conn
	started  chan struct{}
	release  chan struct{}
	writeErr error
	once     sync.Once
}

func (c *controlledFailureConn) Write([]byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	<-c.release
	return 0, c.writeErr
}

func TestTakeoverDoesNotHideIndependentWriteError(t *testing.T) {
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	failure := errors.New("independent socket failure")
	conn := &controlledFailureConn{Conn: server, started: make(chan struct{}), release: make(chan struct{}), writeErr: failure}
	c := &client{conn: conn, status: Connected}
	done := make(chan error, 1)
	go func() { done <- c.WriterPacket(packets.NewControlPacket(packets.Pingresp)) }()
	<-conn.started
	end := c.beginTakeover()
	defer end()
	close(conn.release)
	err := <-done
	if !errors.Is(err, failure) || errors.Is(err, errTakeoverWrite) {
		t.Fatalf("write error = %v", err)
	}
}

func TestSharedCandidateRetryOnlyBeforeWrite(t *testing.T) {
	for _, started := range []bool{false, true} {
		var visited []int
		err := deliverSharedCandidates([]int{0, 1}, 0, func(i int) (bool, error) {
			visited = append(visited, i)
			if i == 0 {
				return started, nil
			}
			return true, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if started && len(visited) != 1 || !started && (len(visited) != 2 || visited[1] != 1) {
			t.Fatalf("started=%t visited=%v", started, visited)
		}
	}
}

func TestUnstartedQoSDeliveryRollsBackBeforeSharedRetry(t *testing.T) {
	s := &sessions.Session{}
	if err := s.Init(&packets.ConnectPacket{ClientIdentifier: "pending"}); err != nil {
		t.Fatal(err)
	}
	owner, _ := s.ClaimOutbound()
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	c := &client{conn: server, status: Connected, session: s, outboundOwner: owner, inflight: make(map[uint16]*inflightElem)}
	end := c.beginTakeover()
	defer end()
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName = "topic"
	err := publish(&subscription{client: c, share: true}, p, 1)
	if !errors.Is(err, errTakeoverWrite) || !errors.Is(err, errWriteNotStarted) {
		t.Fatalf("error = %v", err)
	}
	_, pending := s.ClaimOutbound()
	if len(pending) != 0 || len(c.inflight) != 0 {
		t.Fatalf("unsent delivery retained: %v", pending)
	}
}

func TestUnstartedOrdinaryDeliveryRemainsInPersistentSession(t *testing.T) {
	s := &sessions.Session{}
	if err := s.Init(&packets.ConnectPacket{ClientIdentifier: "persistent"}); err != nil {
		t.Fatal(err)
	}
	owner, _ := s.ClaimOutbound()
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	c := &client{conn: server, status: Connected, session: s, outboundOwner: owner, inflight: make(map[uint16]*inflightElem)}
	end := c.beginTakeover()
	defer end()
	defer c.Close()
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Payload = "topic", []byte("preserved")
	err := publish(&subscription{client: c}, p, 1)
	if !errors.Is(err, errTakeoverWrite) || !errors.Is(err, errWriteNotStarted) {
		t.Fatalf("error = %v", err)
	}
	_, recovered := s.ClaimOutbound()
	if len(recovered) != 1 {
		t.Fatalf("pending messages = %v", recovered)
	}
	for _, msg := range recovered {
		if string(msg.Packet.Payload) != "preserved" {
			t.Fatalf("packet = %#v", msg.Packet)
		}
	}
}

func TestSharedRollbackInvalidatesCopiedRetry(t *testing.T) {
	s := &sessions.Session{}
	if err := s.Init(&packets.ConnectPacket{ClientIdentifier: "shared-retry"}); err != nil {
		t.Fatal(err)
	}
	owner, _ := s.ClaimOutbound()
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	conn := &publishSignalConn{Conn: server, started: make(chan struct{})}
	c := &client{conn: conn, status: Connected, session: s, outboundOwner: owner, inflight: make(map[uint16]*inflightElem)}
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName = "topic"
	published := make(chan error, 1)
	go func() { published <- publish(&subscription{client: c, share: true}, p, 1) }()
	select {
	case <-conn.started:
	case <-time.After(time.Second):
		t.Fatal("first write did not start")
	}
	c.inflightMu.RLock()
	var id uint16
	var snapshot *inflightElem
	for id, snapshot = range c.inflight {
		break
	}
	c.inflightMu.RUnlock()
	if snapshot == nil {
		t.Fatal("missing retry snapshot")
	}
	retryDone := make(chan struct{})
	go func() { c.retryInflight(id, snapshot, Publish, snapshot.packet, time.Now().Unix()); close(retryDone) }()
	end := c.beginTakeover()
	select {
	case err := <-published:
		if !errors.Is(err, errWriteZeroBytes) {
			t.Fatalf("publish = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("publish did not unblock")
	}
	end()
	select {
	case <-retryDone:
	case <-time.After(time.Second):
		t.Fatal("stale retry did not finish")
	}
	// A reused packet ID is a different delivery, even if its status matches.
	replacement := &inflightElem{status: Publish, packet: snapshot.packet, timestamp: 1}
	c.inflightMu.Lock()
	c.inflight[id] = replacement
	c.inflightMu.Unlock()
	c.retryInflight(id, snapshot, Publish, snapshot.packet, time.Now().Unix())
	if replacement.timestamp != 1 {
		t.Fatal("stale retry updated the replacement")
	}
	_ = peer.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
	if packet, err := packets.ReadPacket(peer); err == nil {
		t.Fatalf("stale retry wrote packet: %#v", packet)
	}
	if len(s.Outbound()) != 0 {
		t.Fatal("rolled-back delivery still in session")
	}
}

func TestSharedZeroByteInterruptionRollsBack(t *testing.T) {
	s := &sessions.Session{}
	if err := s.Init(&packets.ConnectPacket{ClientIdentifier: "shared"}); err != nil {
		t.Fatal(err)
	}
	owner, _ := s.ClaimOutbound()
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	conn := &publishSignalConn{Conn: server, started: make(chan struct{})}
	c := &client{conn: conn, status: Connected, session: s, outboundOwner: owner, inflight: make(map[uint16]*inflightElem)}
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName = "topic"
	done := make(chan error, 1)
	go func() { done <- publish(&subscription{client: c, share: true}, p, 1) }()
	select {
	case <-conn.started:
	case <-time.After(time.Second):
		t.Fatal("write did not start")
	}
	end := c.beginTakeover()
	defer end()
	select {
	case err := <-done:
		if !errors.Is(err, errTakeoverWrite) || !errors.Is(err, errWriteZeroBytes) {
			t.Fatalf("error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("write did not unblock")
	}
	_, pending := s.ClaimOutbound()
	if len(pending) != 0 || len(c.inflight) != 0 {
		t.Fatalf("zero-byte delivery retained: %v", pending)
	}
}

func TestConcurrentSubscriberCloseDoesNotFailPublisher(t *testing.T) {
	b, err := NewBroker(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	conn := &controlledFailureConn{Conn: server, started: make(chan struct{}), release: make(chan struct{}), writeErr: net.ErrClosed}
	c := &client{conn: conn, status: Connected, info: info{clientID: "closing"}}
	sub := &subscription{client: c, topic: "closing/topic", qos: 0}
	c.subMap = map[string]*subscription{sub.topic: sub}
	b.clients.Store(c.info.clientID, c)
	defer b.clients.Delete(c.info.clientID)
	if _, err := b.topicsMgr.Subscribe([]byte(sub.topic), 0, sub); err != nil {
		t.Fatal(err)
	}
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName = sub.topic
	done := make(chan error, 1)
	go func() { done <- b.publishLocal(p) }()
	select {
	case <-conn.started:
	case <-time.After(time.Second):
		t.Fatal("write did not start")
	}
	c.Close()
	close(conn.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("subscriber close escaped: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("publish blocked")
	}
}
