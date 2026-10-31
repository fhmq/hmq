package broker

import (
	"net"
	"testing"
	"time"

	"github.com/fhmq/hmq/v4/broker/packets"
)

func TestRetryAndAcknowledgmentOrdering(t *testing.T) {
	for _, tc := range []struct {
		name   string
		qos    byte
		status InflightStatus
		ack    byte
	}{
		{"puback", 1, Publish, packets.Puback},
		{"pubrec", 2, Publish, packets.Pubrec},
		{"pubcomp", 2, Pubrel, packets.Pubcomp},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, peer := net.Pipe()
			defer server.Close()
			defer peer.Close()
			_ = peer.SetDeadline(time.Now().Add(3 * time.Second))
			p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			p.TopicName, p.Qos, p.MessageID = "retry/topic", tc.qos, 7
			elem := &inflightElem{status: tc.status, packet: p}
			c := &client{conn: server, status: Connected, inflight: map[uint16]*inflightElem{7: elem}}
			c.writeMu.Lock()
			writeLocked := true
			defer func() {
				if writeLocked {
					c.writeMu.Unlock()
				}
			}()
			retryDone := make(chan struct{})
			go func() { c.retryInflight(7, elem, tc.status, p, time.Now().Unix()); close(retryDone) }()
			deadline := time.Now().Add(time.Second)
			for {
				if !c.outboundSendMu.TryLock() {
					break
				}
				c.outboundSendMu.Unlock()
				if time.Now().After(deadline) {
					t.Fatal("retry did not acquire send lock")
				}
				time.Sleep(time.Millisecond)
			}
			ack := packets.NewControlPacket(tc.ack)
			switch a := ack.(type) {
			case *packets.PubackPacket:
				a.MessageID = 7
			case *packets.PubrecPacket:
				a.MessageID = 7
			case *packets.PubcompPacket:
				a.MessageID = 7
			}
			ackStarted, ackDone := make(chan struct{}), make(chan struct{})
			go func() { close(ackStarted); ProcessMessage(&Message{client: c, packet: ack}); close(ackDone) }()
			<-ackStarted
			select {
			case <-ackDone:
				t.Fatal("acknowledgment overtook the retry")
			case <-time.After(50 * time.Millisecond):
			}
			c.inflightMu.RLock()
			current := c.inflight[7]
			unchanged := current == elem && current.status == tc.status
			c.inflightMu.RUnlock()
			if !unchanged {
				t.Fatal("acknowledgment changed state before retry completed")
			}
			c.writeMu.Unlock()
			writeLocked = false
			first, err := packets.ReadPacket(peer)
			if err != nil {
				t.Fatal(err)
			}
			if tc.status == Publish {
				if p, ok := first.(*packets.PublishPacket); !ok || !p.Dup {
					t.Fatalf("retry = %#v", first)
				}
			} else if _, ok := first.(*packets.PubrelPacket); !ok {
				t.Fatalf("retry = %#v", first)
			}
			if tc.ack == packets.Pubrec {
				second, err := packets.ReadPacket(peer)
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := second.(*packets.PubrelPacket); !ok {
					t.Fatalf("after PUBREC = %#v", second)
				}
			}
			select {
			case <-retryDone:
			case <-time.After(time.Second):
				t.Fatal("retry blocked")
			}
			select {
			case <-ackDone:
			case <-time.After(time.Second):
				t.Fatal("ack blocked")
			}
			c.retryInflight(7, elem, tc.status, p, time.Now().Unix())
			_ = peer.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			if got, err := packets.ReadPacket(peer); err == nil {
				t.Fatalf("stale retry after ack = %#v", got)
			}
		})
	}
}

func TestTakeoverUnblocksRetryAndPubrec(t *testing.T) {
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	conn := &publishSignalConn{Conn: server, started: make(chan struct{})}
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Qos, p.MessageID = "retry/topic", 2, 7
	elem := &inflightElem{status: Publish, packet: p}
	c := &client{conn: conn, status: Connected, inflight: map[uint16]*inflightElem{7: elem}}
	retryDone := make(chan struct{})
	go func() { c.retryInflight(7, elem, Publish, p, time.Now().Unix()); close(retryDone) }()
	select {
	case <-conn.started:
	case <-time.After(time.Second):
		t.Fatal("retry did not start writing")
	}
	ack := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
	ack.MessageID = 7
	ackDone := make(chan struct{})
	go func() { ProcessMessage(&Message{client: c, packet: ack}); close(ackDone) }()
	end := c.beginTakeover()
	defer end()
	select {
	case <-retryDone:
	case <-time.After(time.Second):
		t.Fatal("takeover did not interrupt retry")
	}
	select {
	case <-ackDone:
	case <-time.After(time.Second):
		t.Fatal("PUBREC blocked during takeover")
	}
	c.inflightMu.RLock()
	status := c.inflight[7].status
	c.inflightMu.RUnlock()
	if status != Pubrel {
		t.Fatalf("status = %v", status)
	}
}
