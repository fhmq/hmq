package broker

import (
	stdjson "encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fhmq/hmq/v4/broker/packets"
	"github.com/fhmq/hmq/v4/broker/replication"
	"github.com/fhmq/hmq/v4/broker/sessionstore"
	"github.com/hashicorp/raft"
)

func TestQuorumCheckpointRestoresHandshakeAndQueue(t *testing.T) {
	configs := quorumTestConfigs(t)
	nodes := make([]*Broker, len(configs))
	open := func(i int) {
		t.Helper()
		b, err := NewBroker(&configs[i])
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = b
		t.Cleanup(b.Close)
	}
	for i := range configs {
		configs[i].Worker = 2
		configs[i].Cluster.Quorum.SessionPersistence = true
		configs[i].SessionQuota = sessionstore.Limits{MaxPendingMessages: 16}
		for j := range configs {
			if i != j {
				configs[i].Cluster.Seeds = append(configs[i].Cluster.Seeds, configs[j].Cluster.Listen)
			}
		}
		open(i)
	}
	leader := quorumLeader(t, nodes)
	b := nodes[leader]
	const topic = "checkpoint/data"
	write := func(c net.Conn, p packets.ControlPacket) {
		t.Helper()
		if err := p.Write(c); err != nil {
			t.Fatal(err)
		}
	}
	read := func(c net.Conn) packets.ControlPacket {
		t.Helper()
		p, err := packets.ReadPacket(c)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	disconnect := func(c net.Conn, done <-chan error) {
		t.Helper()
		c.Close()
		select {
		case <-done:
		case <-time.After(6 * time.Second):
			t.Fatal("connection did not stop")
		}
	}
	sub, done := logMQTTConnect(t, b, "checkpoint-sub", false)
	legacyLoadSubscribe(t, sub, topic, 2)
	disconnect(sub, done)
	pub, done := logMQTTConnect(t, b, "checkpoint-pub", false)
	original := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	original.TopicName, original.Qos, original.MessageID, original.Payload = topic, 2, 1, []byte("outbound")
	if err := legacyLoadPublish(pub, original); err != nil {
		t.Fatal(err)
	}
	original.MessageID, original.Payload = 2, []byte("inbound")
	write(pub, original)
	if p, ok := read(pub).(*packets.PubrecPacket); !ok || p.MessageID != 2 {
		t.Fatalf("PUBREC: %#v", p)
	}
	disconnect(pub, done)
	sub, done = logMQTTConnect(t, b, "checkpoint-sub", true)
	delivery, ok := read(sub).(*packets.PublishPacket)
	if !ok || string(delivery.Payload) != "outbound" {
		t.Fatalf("outbound PUBLISH: %#v", delivery)
	}
	rec := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
	rec.MessageID = delivery.MessageID
	write(sub, rec)
	if p, ok := read(sub).(*packets.PubrelPacket); !ok || p.MessageID != rec.MessageID {
		t.Fatalf("outbound PUBREL: %#v", p)
	}
	disconnect(sub, done)
	pending := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	pending.TopicName, pending.Qos, pending.Payload = topic, 1, []byte("queued")
	if err := b.PublishMessage(pending); err != nil {
		t.Fatal(err)
	}

	journal := b.quorumRuntime.node
	barrier := []byte(`{"SessionOp":"barrier"}`)
	var serial int
	fill := func(count int) uint64 {
		t.Helper()
		var last uint64
		type result struct {
			index uint64
			err   error
		}
		for count > 0 {
			batch := min(count, 8)
			results := make(chan result, batch)
			for range batch {
				serial++
				go func(id int) {
					var index uint64
					var err error
					for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
						index, err = journal.PublishAt(fmt.Sprintf("checkpoint-test-%d", id), barrier, journal.Floor(), 8*time.Second)
						if !errors.Is(err, replication.ErrCompacted) && !errors.Is(err, replication.ErrTimeout) {
							break
						}
					}
					results <- result{index, err}
				}(serial)
			}
			for range batch {
				r := <-results
				if r.err != nil {
					t.Fatalf("fill history: %v (leader state %s, capacity %+v)", r.err, journal.State(), journal.Capacity())
				}
				last = max(last, r.index)
			}
			count -= batch
		}
		return last
	}
	wait := func(predicate func() bool, label string) {
		t.Helper()
		for deadline := time.Now().Add(25 * time.Second); time.Now().Before(deadline); {
			if predicate() {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		for i, node := range nodes {
			t.Logf("node %d: health=%+v capacity=%+v last=%d", i, node.quorumRuntime.health(), node.quorumRuntime.node.Capacity(), node.quorumRuntime.node.LastIndex())
		}
		t.Fatal(label)
	}
	first := fill(150)
	wait(func() bool { return journal.Floor() > 0 }, "history was not reclaimed")
	floor := journal.Floor()
	if journal.Capacity().Entries >= 128 {
		t.Fatal("compaction did not reclaim admission capacity")
	}
	if _, err := journal.PublishAt("checkpoint-test-1", barrier, 0, time.Second); !errors.Is(err, replication.ErrCompacted) {
		t.Fatalf("stale operation was not fenced: %v", err)
	}

	slow := (leader + 1) % len(nodes)
	quorumWaitCursor(t, nodes[slow], first)
	oldLast := nodes[slow].quorumRuntime.node.LastIndex()
	nodes[slow].Close()
	fill(160)
	pending.Payload = []byte("published while follower was down")
	if err := b.PublishMessage(pending); err != nil {
		t.Fatal(err)
	}
	last := journal.HistoryTip()
	quorumWaitCursor(t, b, last)
	t.Logf("compacted through %d, offline follower last index %d, leader wrote through %d", floor, oldLast, last)
	if got := journal.Floor(); got > oldLast || got < floor {
		t.Fatalf("unsafe compaction while follower offline: floor=%d follower=%d", got, oldLast)
	}
	if err := journal.CheckpointWAL(); err != nil {
		t.Fatal(err)
	}
	open(slow)
	wait(func() bool {
		return nodes[slow].quorumRuntime.health().Cursor >= last
	}, "slow follower did not replay retained suffix")
	dir := filepath.Join(configs[slow].Cluster.Quorum.DataDir, "snapshots")
	snapshots, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var installed uint64
	for _, snap := range snapshots {
		data, err := os.ReadFile(filepath.Join(dir, snap.Name(), "meta.json"))
		var meta raft.SnapshotMeta
		if err == nil && stdjson.Unmarshal(data, &meta) == nil {
			installed = max(installed, meta.Index)
		}
	}
	if installed < last {
		t.Fatalf("slow follower did not obtain snapshot containing suffix: latest %d, want at least %d", installed, last)
	}

	// Force a snapshot concurrently with shutdown; an interrupted local
	// snapshot cannot invalidate the committed checkpoint on surviving peers.
	j := make(chan error, 1)
	go func() { j <- journal.CheckpointWAL() }()
	b.Close()
	select {
	case <-j:
	case <-time.After(8 * time.Second):
		t.Fatal("snapshot did not finish during leader shutdown")
	}
	survivors := []*Broker{nodes[slow], nodes[3-leader-slow]}
	next := survivors[quorumLeader(t, survivors)]
	// The stopped leader may not yet have reported its final cursor. Its
	// report can pin compaction, but must not prevent majority progress.
	if _, err := next.quorumRuntime.sessions.commit(quorumSessionCommand{SessionOp: "barrier"}); err != nil {
		t.Fatalf("surviving majority cannot commit: %v", err)
	}

	for _, node := range survivors {
		node.Close()
	}
	for i := range nodes {
		open(i)
	}
	b = nodes[quorumLeader(t, nodes)]
	wait(func() bool { return b.quorumRuntime.node.Floor() > floor }, "recovered members did not reclaim caught-up suffix")
	sub, done = logMQTTConnect(t, b, "checkpoint-sub", true)
	if p, ok := read(sub).(*packets.PubrelPacket); !ok || p.MessageID != rec.MessageID {
		t.Fatalf("recovered PUBREL: %#v", p)
	}
	comp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
	comp.MessageID = rec.MessageID
	queued, ok := read(sub).(*packets.PublishPacket)
	if !ok || string(queued.Payload) != "queued" {
		t.Fatalf("recovered queue: %#v", queued)
	}
	late, ok := read(sub).(*packets.PublishPacket)
	if !ok || string(late.Payload) != "published while follower was down" || late.MessageID == queued.MessageID {
		t.Fatalf("replayed suffix: %#v", late)
	}
	write(sub, comp)
	ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
	ack.MessageID = queued.MessageID
	write(sub, ack)
	ack.MessageID = late.MessageID
	write(sub, ack)
	write(sub, packets.NewControlPacket(packets.Pingreq))
	if p := read(sub); p == nil {
		t.Fatal("missing PINGRESP")
	} else if _, ok := p.(*packets.PingrespPacket); !ok {
		t.Fatalf("unexpected duplicate before PINGRESP: %#v", p)
	}
	disconnect(sub, done)

	pub, done = logMQTTConnect(t, b, "checkpoint-pub", true)
	rel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
	rel.MessageID = 2
	for range 2 {
		write(pub, rel)
		if p, ok := read(pub).(*packets.PubcompPacket); !ok || p.MessageID != 2 {
			t.Fatalf("recovered inbound PUBCOMP: %#v", p)
		}
	}
	disconnect(pub, done)
	state, _, err := b.sessionLog.Get("checkpoint-sub")
	if err != nil || len(state.Queue) != 1 || len(state.Inflight) != 0 || string(state.Queue[0].Payload) != "inbound" {
		t.Fatalf("recovered fanout duplicated or lost: %#v, %v", state, err)
	}
}
