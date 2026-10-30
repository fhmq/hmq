package broker

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/v3/broker/replication"
	"github.com/hashicorp/raft"
)

func quorumConnect(t *testing.T, b *Broker, id string) net.Conn {
	t.Helper()
	var address string
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		b.lifecycleMu.Lock()
		for l := range b.listeners {
			address = l.Addr().String()
		}
		b.lifecycleMu.Unlock()
		if address != "" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	c, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = c.SetDeadline(time.Now().Add(8 * time.Second))
	p := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	p.ProtocolName, p.ProtocolVersion, p.CleanSession, p.ClientIdentifier = "MQTT", 4, true, id
	if err := p.Write(c); err != nil {
		t.Fatal(err)
	}
	ack, err := packets.ReadPacket(c)
	if err != nil {
		t.Fatal(err)
	}
	if a, ok := ack.(*packets.ConnackPacket); !ok || a.ReturnCode != 0 {
		t.Fatalf("CONNACK: %#v", ack)
	}
	return c
}

func quorumSubscribe(t *testing.T, c net.Conn, topic string) {
	t.Helper()
	p := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	p.MessageID, p.Topics, p.Qoss = 1, []string{topic}, []byte{0}
	if err := p.Write(c); err != nil {
		t.Fatal(err)
	}
	ack, err := packets.ReadPacket(c)
	if err != nil {
		t.Fatal(err)
	}
	if a, ok := ack.(*packets.SubackPacket); !ok || len(a.ReturnCodes) != 1 || a.ReturnCodes[0] != 0 {
		t.Fatalf("SUBACK: %#v", ack)
	}
}

func quorumWrite(t *testing.T, c net.Conn, payload string, qos byte) {
	t.Helper()
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Payload, p.Qos, p.MessageID = "quorum/test", []byte(payload), qos, 42
	if err := p.Write(c); err != nil {
		t.Fatal(err)
	}
}

func quorumReadDelivery(t *testing.T, c net.Conn, payload string) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(8 * time.Second))
	p, err := packets.ReadPacket(c)
	if err != nil {
		t.Fatal(err)
	}
	if p, ok := p.(*packets.PublishPacket); !ok || string(p.Payload) != payload || p.Retain {
		t.Fatalf("delivery: %#v", p)
	}
}

func quorumLeader(t *testing.T, nodes []*Broker) int {
	t.Helper()
	for deadline := time.Now().Add(12 * time.Second); time.Now().Before(deadline); {
		for i, b := range nodes {
			if b.quorumRuntime.node.State() == raft.Leader {
				return i
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no quorum leader")
	return -1
}

func quorumWaitCursor(t *testing.T, b *Broker, index uint64) {
	t.Helper()
	for deadline := time.Now().Add(8 * time.Second); time.Now().Before(deadline); {
		if b.quorumRuntime.health().Cursor >= index {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("cursor did not advance: %+v", b.quorumRuntime.health())
}

func TestQuorumMQTTPublishAndRestart(t *testing.T) {
	configs := quorumTestConfigs(t)
	nodes := make([]*Broker, 3)
	start := func(i int) {
		configs[i].Host, configs[i].Port = "127.0.0.1", "0"
		b, err := NewBroker(&configs[i])
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = b
		t.Cleanup(b.Close)
		b.Start()
	}
	for i := range nodes {
		start(i)
	}
	leader := quorumLeader(t, nodes)
	zeroID := quorumConnect(t, nodes[leader], "invalid-zero-id")
	invalid := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	invalid.TopicName, invalid.Payload, invalid.Qos, invalid.MessageID = "quorum/test", []byte("invalid-id"), 1, 0
	if err := invalid.Write(zeroID); err != nil {
		t.Fatal(err)
	}
	if p, err := packets.ReadPacket(zeroID); err == nil {
		t.Fatalf("ACK for zero packet ID: %#v", p)
	} else if e, ok := err.(net.Error); ok && e.Timeout() {
		t.Fatal("zero ID publisher not disconnected")
	}
	subs := make([]net.Conn, 3)
	for i, b := range nodes {
		subs[i] = quorumConnect(t, b, fmt.Sprintf("sub-%d", i))
		quorumSubscribe(t, subs[i], "quorum/test")
	}
	pub := quorumConnect(t, nodes[leader], "publisher")
	// Occupied proposal capacity must reject immediately, without a hidden queue,
	// PUBACK, or local delivery. The same leader accepts after capacity is freed.
	func() {
		for i := 0; i < cap(nodes[leader].quorumRuntime.proposals); i++ {
			nodes[leader].quorumRuntime.proposals <- struct{}{}
		}
		defer func() {
			for i := 0; i < cap(nodes[leader].quorumRuntime.proposals); i++ {
				<-nodes[leader].quorumRuntime.proposals
			}
		}()
		var wg sync.WaitGroup
		for i := 0; i < 64; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := nodes[leader].PublishMessage(&packets.PublishPacket{TopicName: "test"}); !errors.Is(err, replication.ErrBusy) {
					t.Errorf("expected busy, got %v", err)
				}
			}()
		}
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("proposals queued behind occupied slot")
		}
		quorumWrite(t, pub, "one", 1)
		_ = pub.SetReadDeadline(time.Now().Add(time.Second))
		if p, err := packets.ReadPacket(pub); err == nil {
			t.Fatalf("early ACK: %#v", p)
		} else if e, ok := err.(net.Error); ok && e.Timeout() {
			t.Fatalf("busy publisher queued instead of disconnected: %v", err)
		}
		entries, err := nodes[leader].quorumRuntime.node.Entries(0, 10)
		if err != nil || len(entries) != 0 {
			t.Fatalf("unexpected precommit entries: %v %v", entries, err)
		}
		for _, s := range subs {
			_ = s.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
			if p, err := packets.ReadPacket(s); err == nil {
				t.Fatalf("early delivery: %#v", p)
			} else if e, ok := err.(net.Error); !ok || !e.Timeout() {
				t.Fatalf("unexpected precommit read: %v", err)
			}
		}
	}()
	pub = quorumConnect(t, nodes[leader], "publisher-after-busy")
	quorumWrite(t, pub, "one", 1)
	_ = pub.SetReadDeadline(time.Now().Add(8 * time.Second))
	ack, err := packets.ReadPacket(pub)
	if err != nil {
		t.Fatal(err)
	}
	if a, ok := ack.(*packets.PubackPacket); !ok || a.MessageID != 42 {
		t.Fatalf("PUBACK: %#v", ack)
	}
	entries, err := nodes[leader].quorumRuntime.node.Entries(0, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("ACK without applied commit: %v %v", entries, err)
	}
	for i := range subs {
		quorumReadDelivery(t, subs[i], "one")
		quorumWaitCursor(t, nodes[i], entries[0].Index)
	}
	quorumWrite(t, pub, "zero", 0)
	for _, s := range subs {
		quorumReadDelivery(t, s, "zero")
	}
	entries, err = nodes[leader].quorumRuntime.node.Entries(0, 10)
	if err != nil || len(entries) != 2 {
		t.Fatalf("QoS0 missing commit: %v %v", entries, err)
	}
	for _, b := range nodes {
		quorumWaitCursor(t, b, entries[1].Index)
	}
	// Without direct cluster seeds a follower cannot forward and must not ACK.
	follower := (leader + 1) % 3
	bad := quorumConnect(t, nodes[follower], "follower-pub")
	quorumWrite(t, bad, "reject-follower", 1)
	if p, err := packets.ReadPacket(bad); err == nil {
		t.Fatalf("follower acknowledged: %#v", p)
	} else if e, ok := err.(net.Error); ok && e.Timeout() {
		t.Fatal("follower did not disconnect")
	}
	// Resume each node from its own fsynced cursor, including a full-cluster restart.
	for _, b := range nodes {
		b.Close()
	}
	for i := range nodes {
		start(i)
		if nodes[i].quorumRuntime.health().Cursor != entries[1].Index {
			t.Fatal("cursor not recovered")
		}
	}
	leader = quorumLeader(t, nodes)
	for i, b := range nodes {
		subs[i] = quorumConnect(t, b, fmt.Sprintf("restarted-sub-%d", i))
		quorumSubscribe(t, subs[i], "quorum/test")
	}
	pub = quorumConnect(t, nodes[leader], "restarted-publisher")
	quorumWrite(t, pub, "after-restart", 1)
	if p, err := packets.ReadPacket(pub); err != nil {
		t.Fatal(err)
	} else if _, ok := p.(*packets.PubackPacket); !ok {
		t.Fatalf("not PUBACK: %#v", p)
	}
	for _, s := range subs {
		quorumReadDelivery(t, s, "after-restart")
	}
	// Remove the majority while keeping the publisher's TCP session alive.
	for i, b := range nodes {
		if i != leader {
			b.Close()
		}
	}
	quorumWrite(t, pub, "no-quorum", 1)
	if p, err := packets.ReadPacket(pub); err == nil {
		t.Fatalf("ACK without quorum: %#v", p)
	} else if e, ok := err.(net.Error); ok && e.Timeout() {
		t.Fatal("proposal failed to terminate")
	}
	entries, err = nodes[leader].quorumRuntime.node.Entries(0, 10)
	if err != nil || len(entries) != 3 {
		t.Fatalf("uncommitted publication became visible: %v %v", entries, err)
	}
}
