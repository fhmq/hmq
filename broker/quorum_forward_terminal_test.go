package broker

import (
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fhmq/hmq/v3/broker/replication"
	"github.com/hashicorp/raft"
)

func TestQuorumForwardTerminalResponses(t *testing.T) {
	for _, tc := range []struct {
		code quorumForwardError
		err  error
	}{
		{quorumForwardCapacity, replication.ErrCapacity},
		{quorumForwardConflict, replication.ErrConflict},
		{quorumForwardInvalid, replication.ErrInvalid},
	} {
		t.Run(string(tc.code), func(t *testing.T) {
			if got := quorumForwardTerminalError(fmt.Errorf("private details: %w", tc.err)); got != tc.code {
				t.Fatalf("wrapped error: %q", got)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			p := &quorumPending{digest: quorumDigest([]byte("data")), attempts: map[uint32]string{1: "old", 2: "leader"}, latest: 2, result: make(chan quorumForwardResult, 1)}
			f := &quorumForward{ctx: ctx, cancel: cancel, members: map[string]string{"old": "", "leader": "", "wrong": ""}, pending: map[string]*quorumPending{"op": p}}
			good := quorumForwardFrameData{Domain: quorumForwardDomain, Response: true, Operation: "op", Attempt: 2, Digest: p.digest, Error: tc.code}
			for i := 0; i < 8; i++ {
				m := good
				peer := "leader"
				switch i {
				case 0:
					peer = "wrong"
				case 1:
					m.Operation = "other"
				case 2:
					m.Digest = quorumDigest(nil)
				case 3:
					m.Attempt = 3
				case 4:
					m.Index = 1
				case 5:
					m.Error = "raw internal error"
				case 6:
					m.Attempt = 1
					peer = "old"
				case 7:
					m.Payload = []byte("unexpected")
				}
				data, _ := stdjson.Marshal(m)
				f.handle(peer, data)
				select {
				case r := <-p.result:
					t.Fatalf("invalid response %d resolved: %+v", i, r)
				default:
				}
			}
			data, _ := stdjson.Marshal(good)
			f.handle("leader", data)
			select {
			case r := <-p.result:
				if r.index != 0 || !errors.Is(r.err, tc.err) {
					t.Fatalf("result: %+v", r)
				}
			default:
				t.Fatal("terminal response ignored")
			}
			// No Error field is required on existing positive responses.
			good.Error = ""
			good.Index = 7
			good.Attempt = 1
			data, _ = stdjson.Marshal(good)
			f.handle("old", data)
			select {
			case r := <-p.result:
				if r.index != 7 || r.err != nil {
					t.Fatalf("positive result: %+v", r)
				}
			default:
				t.Fatal("earlier success ignored")
			}
		})
	}
	for _, err := range []error{nil, replication.ErrBusy, replication.ErrTimeout, raft.ErrNotLeader, context.DeadlineExceeded, errors.New("private error")} {
		if code := quorumForwardTerminalError(err); code != "" {
			t.Fatalf("retryable error %v classified %q", err, code)
		}
	}
}

func TestQuorumForwardTerminalConflict(t *testing.T) {
	configs := quorumTestConfigs(t)
	for i := range configs {
		for j := range configs {
			if i != j {
				configs[i].Cluster.Seeds = append(configs[i].Cluster.Seeds, configs[j].Cluster.Listen)
			}
		}
	}
	nodes := make([]*Broker, len(configs))
	for i := range nodes {
		b, err := NewBroker(&configs[i])
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = b
		t.Cleanup(b.Close)
	}
	leader := quorumLeader(t, nodes)
	for _, b := range nodes {
		deadline := time.Now().Add(8 * time.Second)
		for len(b.clusterNode.Peers()) != 2 {
			if time.Now().After(deadline) {
				t.Fatal("incomplete direct peers")
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	payload, _ := stdjson.Marshal(clusterPublish{Topic: "terminal", Payload: []byte("new")})
	previous, _ := stdjson.Marshal(clusterPublish{Topic: "terminal", Payload: []byte("old")})
	for _, i := range []int{leader, (leader + 1) % 3} {
		f := nodes[i].quorumRuntime.forward
		op := fmt.Sprintf("%s:%s:%d", f.id, f.boot, f.seq.Load()+1)
		if index, err := nodes[leader].quorumRuntime.node.Publish(op, previous, time.Second); err != nil || index == 0 {
			t.Fatalf("seed conflict: %d %v", index, err)
		}
		var sends atomic.Int32
		original := f.send
		f.send = func(ctx context.Context, id string, data []byte) error { sends.Add(1); return original(ctx, id, data) }
		started := time.Now()
		err := f.publish(payload)
		if !errors.Is(err, replication.ErrConflict) {
			t.Fatalf("node %d: %v", i, err)
		}
		if elapsed := time.Since(started); elapsed > time.Second {
			t.Fatalf("terminal error retried for %s", elapsed)
		}
		if sends.Load() > 1 {
			t.Fatalf("terminal error retried %d sends", sends.Load())
		}
	}
	// An invalid local Raft operation must also return without retrying.
	f := nodes[leader].quorumRuntime.forward
	f.boot = strings.Repeat("x", 256)
	started := time.Now()
	if err := f.publish(payload); !errors.Is(err, replication.ErrInvalid) {
		t.Fatalf("local invalid: %v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("invalid local proposal retried")
	}
}
