package broker

import (
	"context"
	stdjson "encoding/json"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"errors"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/v3/broker/replication"
	"github.com/fhmq/hmq/v3/broker/storage"
)

func TestQuorumForwardMQTT(t *testing.T) {
	configs := quorumTestConfigs(t)
	for i := range configs {
		for j := range configs {
			if i != j {
				configs[i].Cluster.Seeds = append(configs[i].Cluster.Seeds, configs[j].Cluster.Listen)
			}
		}
	}
	nodes := make([]*Broker, 3)
	for i := range nodes {
		configs[i].Host = "127.0.0.1"
		configs[i].Port = "0"
		b, err := NewBroker(&configs[i])
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = b
		t.Cleanup(b.Close)
		b.Start()
	}
	leader := quorumLeader(t, nodes)
	follower := (leader + 1) % 3
	// Raft connectivity cannot substitute for direct authenticated cluster peers.
	for _, b := range nodes {
		deadline := time.Now().Add(8 * time.Second)
		for len(b.clusterNode.Peers()) != 2 {
			if time.Now().After(deadline) {
				t.Fatalf("cluster peers incomplete: %v", b.clusterNode.Peers())
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	sub := quorumConnect(t, nodes[follower], "forward-sub")
	quorumSubscribe(t, sub, "quorum/test")
	pub := quorumConnect(t, nodes[follower], "forward-pub")
	originForward := nodes[follower].quorumRuntime.forward
	originSend := originForward.send
	var requests atomic.Int32
	var lastBudget atomic.Int64
	var firstOperation atomic.Value
	originForward.send = func(ctx context.Context, id string, data []byte) error {
		var m quorumForwardFrameData
		_ = stdjson.Unmarshal(data, &m)
		if !m.Response {
			n := requests.Add(1)
			if n == 1 {
				firstOperation.Store(m.Operation)
			}
			if previous := lastBudget.Swap(m.BudgetNanos); firstOperation.Load() == m.Operation && previous != 0 && m.BudgetNanos >= previous {
				t.Error("retry reset remaining budget")
			}
			if n == 1 {
				select {
				case <-time.After(250 * time.Millisecond):
				case <-ctx.Done():
					t.Error("slow send received short subdeadline")
					return ctx.Err()
				}
			}
		}
		return originSend(ctx, id, data)
	}
	var dropped atomic.Bool
	f := nodes[leader].quorumRuntime.forward
	original := f.send
	// Replace only before sending requests; workers do not access send until work arrives.
	f.send = func(ctx context.Context, id string, p []byte) error {
		var m quorumForwardFrameData
		_ = stdjson.Unmarshal(p, &m)
		if m.Response && dropped.CompareAndSwap(false, true) {
			return nil
		}
		return original(ctx, id, p)
	}
	quorumWrite(t, pub, "deduplicated", 1)
	ack, err := packets.ReadPacket(pub)
	if err != nil {
		t.Fatal(err)
	}
	if a, ok := ack.(*packets.PubackPacket); !ok || a.MessageID != 42 {
		t.Fatalf("ACK: %#v", ack)
	}
	quorumReadDelivery(t, sub, "deduplicated")
	committed, err := nodes[leader].quorumRuntime.node.Entries(0, 100)
	if err != nil || len(committed) != 1 {
		t.Fatalf("leader commit: %v %v", committed, err)
	}
	for _, b := range nodes {
		quorumWaitCursor(t, b, committed[0].Index)
		entries, err := b.quorumRuntime.node.Entries(0, 100)
		if err != nil || len(entries) != 1 {
			t.Fatalf("dedup: %v %v", entries, err)
		}
	}
	if !dropped.Load() {
		t.Fatal("response was not dropped")
	}
	entries, err := nodes[leader].quorumRuntime.node.Entries(0, 10)
	if err != nil || len(entries) != 1 {
		t.Fatal("missing commit", err)
	}
	if index, err := nodes[leader].quorumRuntime.node.Publish(entries[0].ID, []byte(`{"conflict":true}`), time.Second); err == nil || index != 0 {
		t.Fatalf("conflicting operation accepted: %d %v", index, err)
	}
	// A new leader must accept a publication from the surviving follower.
	nodes[leader].Close()
	survivors := []*Broker{nodes[(leader+1)%3], nodes[(leader+2)%3]}
	next := quorumLeader(t, survivors)
	origin := survivors[1-next]
	pub = quorumConnect(t, origin, "after-election")
	quorumWrite(t, pub, "new-leader", 1)
	if ack, err = packets.ReadPacket(pub); err != nil {
		t.Fatal(err)
	} else if _, ok := ack.(*packets.PubackPacket); !ok {
		t.Fatalf("ACK: %#v", ack)
	}
	// No leader connection: a pending MQTT publication must be woken by Close.
	survivors[next].Close()
	pub = quorumConnect(t, origin, "closing")
	quorumWrite(t, pub, "uncertain", 1)
	time.Sleep(50 * time.Millisecond)
	done := make(chan struct{})
	go func() { origin.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(7 * time.Second):
		t.Fatal("shutdown blocked")
	}
	if ack, err = packets.ReadPacket(pub); err == nil {
		t.Fatalf("false ACK on close: %#v", ack)
	}
}

func TestQuorumForwardResponseIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := &quorumForward{ctx: ctx, cancel: cancel, members: map[string]string{"leader": "", "wrong": ""}, pending: make(map[string]*quorumPending)}
	p := &quorumPending{digest: quorumDigest([]byte("payload")), attempts: map[uint32]string{1: "leader", 2: "wrong"}, result: make(chan quorumForwardResult, 1)}
	f.pending["origin:boot:1"] = p
	good := quorumForwardFrameData{Domain: quorumForwardDomain, Response: true, Operation: "origin:boot:1", Attempt: 1, Digest: p.digest, Index: 9}
	for i := 0; i < 5; i++ {
		m := good
		peer := "leader"
		switch i {
		case 0:
			peer = "wrong"
		case 1:
			m.Digest = fmt.Sprintf("%064d", 0)
		case 2:
			m.Attempt = 3
		case 3:
			m.Operation = "unknown"
		case 4:
			m.Index = 0
		}
		data, _ := stdjson.Marshal(m)
		f.handle(peer, data)
		select {
		case <-p.result:
			t.Fatalf("invalid response %d accepted", i)
		default:
		}
	}
	data, _ := stdjson.Marshal(good)
	f.handle("leader", data)
	select {
	case <-p.result:
	default:
		t.Fatal("earlier leader success rejected")
	}
	f.BeginClose()
	f.handle("leader", data)
	select {
	case <-p.result:
		t.Fatal("response after close")
	default:
	}
}

func TestQuorumForwardBudgets(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := &quorumForward{id: "origin", boot: "boot", ctx: ctx, cancel: cancel, pending: make(map[string]*quorumPending)}
	for i := 0; i < quorumProposalLimit; i++ {
		f.pending[fmt.Sprint(i)] = nil
	}
	if err := f.publish([]byte("payload")); !errors.Is(err, replication.ErrBusy) {
		t.Fatalf("count: %v", err)
	}
	clear(f.pending)
	f.bytes = quorumForwardBytes
	if err := f.publish([]byte("payload")); !errors.Is(err, replication.ErrBusy) {
		t.Fatalf("bytes: %v", err)
	}
	if err := f.publish(make([]byte, quorumForwardFrame)); !errors.Is(err, replication.ErrInvalid) {
		t.Fatalf("wire size: %v", err)
	}
	f.BeginClose()
	if err := f.publish([]byte("payload")); !errors.Is(err, storage.ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
}

func TestQuorumForwardRelativeBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := &quorumForward{ctx: ctx, cancel: cancel, members: map[string]string{"origin": ""}, pending: make(map[string]*quorumPending), peers: make(map[string]int), requests: make(chan quorumWork, 4)}
	payload, _ := stdjson.Marshal(clusterPublish{Topic: "test", Payload: []byte("relative")})
	m := quorumForwardFrameData{Domain: quorumForwardDomain, Operation: "origin:0123456789abcdef0123456789abcdef:1", Attempt: 1, Digest: quorumDigest(payload), Payload: payload, BudgetNanos: int64(time.Hour)}
	data, _ := stdjson.Marshal(m)
	var fields map[string]interface{}
	_ = stdjson.Unmarshal(data, &fields)
	if _, ok := fields["Deadline"]; ok {
		t.Fatal("protocol contains absolute deadline")
	}
	before := time.Now()
	f.handle("origin", data)
	select {
	case w := <-f.requests:
		if w.deadline.Before(before) || w.deadline.After(time.Now().Add(5*time.Second)) {
			t.Fatal("budget not clamped")
		}
	case <-time.After(time.Second):
		t.Fatal("valid relative budget rejected")
	}
	m.BudgetNanos = int64(10 * time.Millisecond)
	m.Attempt = 2
	data, _ = stdjson.Marshal(m)
	f.handle("origin", data)
	time.Sleep(20 * time.Millisecond)
	select {
	case w := <-f.requests:
		if time.Until(w.deadline) > 0 {
			t.Fatal("queue wait reset budget")
		}
	default:
		t.Fatal("missing queued request")
	}
}
