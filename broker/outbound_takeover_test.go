package broker

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/v3/broker/lib/sessions"
)

type blockedConnackConn struct {
	net.Conn
	started chan struct{}
	release chan struct{}
}

func (c *blockedConnackConn) Write([]byte) (int, error) {
	close(c.started)
	<-c.release
	return 0, errors.New("injected CONNACK failure")
}

func sendTestConnect(t *testing.T, c net.Conn, id string, clean bool) {
	t.Helper()
	p := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	p.ProtocolName, p.ProtocolVersion, p.ClientIdentifier, p.CleanSession = "MQTT", 4, id, clean
	if err := p.Write(c); err != nil {
		t.Fatal(err)
	}
}

func TestFailedConnackPreservesOwnerAndOtherClientProgress(t *testing.T) {
	for _, clean := range []bool{false, true} {
		t.Run(map[bool]string{false: "resume", true: "clean"}[clean], func(t *testing.T) {
			b, err := NewBroker(&Config{})
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			s, err := b.sessionMgr.New("old")
			if err != nil {
				t.Fatal(err)
			}
			connect := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
			connect.ClientIdentifier = "old"
			if err := s.Init(connect); err != nil {
				t.Fatal(err)
			}
			owner, _ := s.ClaimOutbound()
			p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			p.Qos, p.MessageID = 2, 7
			s.UpdateOutbound(owner, 7, &sessions.OutboundMessage{Packet: p})
			oldServer, oldPeer := net.Pipe()
			defer oldPeer.Close()
			old := &client{broker: b, conn: oldServer, typ: CLIENT, session: s, outboundOwner: owner, info: info{clientID: "old"}}
			old.init()
			old.inflight[7] = &inflightElem{packet: p, status: Pubrel}
			s.UpdateOutbound(owner, 7, &sessions.OutboundMessage{Packet: p, AwaitingPubcomp: true})
			b.clients.Store("old", old)
			defer old.Close()
			server, peer := net.Pipe()
			defer peer.Close()
			blocked := &blockedConnackConn{Conn: server, started: make(chan struct{}), release: make(chan struct{})}
			done := make(chan error, 1)
			go func() { done <- b.handleConnection(CLIENT, blocked) }()
			sendTestConnect(t, peer, "old", clean)
			select {
			case <-blocked.started:
			case <-time.After(time.Second):
				t.Fatal("CONNACK write did not start")
			}
			otherServer, otherPeer := net.Pipe()
			defer otherPeer.Close()
			otherDone := make(chan error, 1)
			go func() { otherDone <- b.handleConnection(CLIENT, otherServer) }()
			otherPeer.SetDeadline(time.Now().Add(time.Second))
			sendTestConnect(t, otherPeer, "other", false)
			if packet, err := packets.ReadPacket(otherPeer); err != nil {
				t.Fatalf("other client blocked: %v", err)
			} else if _, ok := packet.(*packets.ConnackPacket); !ok {
				t.Fatalf("unexpected packet %T", packet)
			}
			close(blocked.release)
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("expected failure")
				}
			case <-time.After(time.Second):
				t.Fatal("failed handshake stuck")
			}
			got, err := b.sessionMgr.Get("old")
			if err != nil || got != s {
				t.Fatal("failed handshake replaced old session")
			}
			comp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
			comp.MessageID = 7
			ProcessMessage(&Message{client: old, packet: comp})
			if len(s.Outbound()) != 0 || old.isDisconnected() {
				t.Fatal("old connection could not acknowledge after failed handshake")
			}
			otherPeer.Close()
			select {
			case <-otherDone:
			case <-time.After(time.Second):
				t.Fatal("other connection stuck")
			}
		})
	}
}

func TestStalePubrecCannotSendPubrel(t *testing.T) {
	b, err := NewBroker(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	server, peer := net.Pipe()
	defer peer.Close()
	c := &client{broker: b, conn: server, typ: CLIENT}
	c.init()
	defer c.Close()
	c.session = &sessions.Session{}
	c.outboundOwner, _ = c.session.ClaimOutbound()
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.Qos, p.MessageID = 2, 7
	c.inflight[7] = &inflightElem{packet: p, status: Publish}
	c.session.UpdateOutbound(c.outboundOwner, 7, &sessions.OutboundMessage{Packet: p})
	_, snapshot := c.session.ClaimOutbound()
	if snapshot[7].AwaitingPubcomp {
		t.Fatal("unexpected state")
	}
	rec := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
	rec.MessageID = 7
	done := make(chan struct{})
	go func() { ProcessMessage(&Message{client: c, packet: rec}); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		server.Close()
		t.Fatal("stale PUBREC attempted a write")
	}
	if c.inflight[7].status != Publish || c.session.Outbound()[7].AwaitingPubcomp {
		t.Fatal("stale PUBREC advanced state")
	}
}
