package replication_test

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/fhmq/hmq/v3/broker/replication"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

const clusterWait = 8 * time.Second

type kernelLogs struct {
	sync.Mutex
	bytes.Buffer
}

func (l *kernelLogs) Write(p []byte) (int, error) {
	l.Lock()
	defer l.Unlock()
	return l.Buffer.Write(p)
}

type kernelCluster struct {
	t          *testing.T
	nodes      [3]*replication.Node
	transports [3]*raft.InmemTransport
	configs    [3]replication.Config
	logs       kernelLogs
}

func newKernelCluster(t *testing.T) *kernelCluster {
	t.Helper()
	c := &kernelCluster{t: t}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("Raft diagnostics:\n%s", c.logs.String())
		}
	})
	t.Cleanup(c.close)
	servers := make([]raft.Server, 3)
	for i := range servers {
		id := fmt.Sprintf("kernel-%d", i)
		addr, transport := raft.NewInmemTransportWithTimeout(raft.ServerAddress(id), time.Second)
		c.transports[i] = transport
		servers[i] = raft.Server{ID: raft.ServerID(id), Address: addr, Suffrage: raft.Voter}
		settings := raft.DefaultConfig()
		// Votes persist term and candidate through real fsync calls. Keep Raft's
		// default election timings; 100ms RPCs/150ms elections caused vote RPC
		// timeouts and repeated terms under race-enabled durable storage tests.
		settings.Logger = hclog.New(&hclog.LoggerOptions{Name: id, Level: hclog.Debug, Output: &c.logs})
		settings.CommitTimeout = 10 * time.Millisecond
		// Only explicit snapshots should affect the persistence test variants.
		settings.SnapshotInterval = time.Hour
		c.configs[i] = replication.Config{ID: id, Dir: t.TempDir(), Transport: transport, RaftConfig: settings}
	}
	c.connect()
	for i := range c.nodes {
		c.configs[i].Bootstrap = servers
		c.open(i)
	}
	return c
}

func (c *kernelCluster) open(i int) {
	c.t.Helper()
	n, err := replication.Open(c.configs[i])
	if err != nil {
		c.t.Fatalf("open node %d: %v", i, err)
	}
	c.nodes[i] = n
}

func (c *kernelCluster) closeNodes() {
	for i, n := range c.nodes {
		if n != nil {
			if err := n.Close(); err != nil {
				c.t.Errorf("close node %d: %v", i, err)
			}
			c.nodes[i] = nil
		}
	}
}

func (c *kernelCluster) close() {
	if c.t.Failed() {
		for i, n := range c.nodes {
			if n != nil {
				addr, id := n.Leader()
				entries, err := n.Entries(0, 1024)
				c.t.Logf("node %d: state=%s leader=%s/%s entries=%v err=%v", i, n.State(), addr, id, entries, err)
			}
		}
	}
	c.closeNodes()
	for i, transport := range c.transports {
		if transport != nil {
			if err := transport.Close(); err != nil {
				c.t.Errorf("close transport %d: %v", i, err)
			}
		}
	}
}

func (c *kernelCluster) connect() {
	for i, from := range c.transports {
		for j, to := range c.transports {
			if i != j {
				from.Connect(to.LocalAddr(), to)
			}
		}
	}
}

func (c *kernelCluster) isolate(i int) {
	for j := range c.transports {
		if i != j {
			c.transports[i].Disconnect(c.transports[j].LocalAddr())
			c.transports[j].Disconnect(c.transports[i].LocalAddr())
		}
	}
}

func kernelEventually(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(clusterWait)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out after %v: %s", clusterWait, description)
}

func (c *kernelCluster) leader(exclude int) int {
	c.t.Helper()
	leader := -1
	kernelEventually(c.t, "elect a unique leader with matching Leader metadata", func() bool {
		leader = -1
		for i, n := range c.nodes {
			if i == exclude || n == nil {
				continue
			}
			if n.State() == raft.Leader {
				if leader != -1 {
					return false
				}
				leader = i
			}
		}
		if leader < 0 {
			return false
		}
		for i, n := range c.nodes {
			if i == exclude || n == nil {
				continue
			}
			addr, id := n.Leader()
			if addr != c.transports[leader].LocalAddr() || id != raft.ServerID(c.configs[leader].ID) {
				return false
			}
		}
		return true
	})
	return leader
}

func (c *kernelCluster) publish(i int, id, payload string) replication.Entry {
	c.t.Helper()
	index, err := c.nodes[i].Publish(id, []byte(payload), time.Second)
	if err != nil || index == 0 {
		c.t.Fatalf("publish %q on %d: index=%d err=%v", id, i, index, err)
	}
	return replication.Entry{Index: index, ID: id, Payload: []byte(payload)}
}

func (c *kernelCluster) converge(want []replication.Entry, indices ...int) {
	c.t.Helper()
	kernelEventually(c.t, "replicas converge to exact committed entries", func() bool {
		for _, i := range indices {
			got, err := c.nodes[i].Entries(0, 1024)
			if err != nil {
				c.t.Fatalf("entries on %d: %v", i, err)
			}
			if !reflect.DeepEqual(got, want) {
				return false
			}
		}
		return true
	})
}

func (c *kernelCluster) retry(i int, e replication.Entry) {
	c.t.Helper()
	index, err := c.nodes[i].Publish(e.ID, e.Payload, time.Second)
	if err != nil || index != e.Index {
		c.t.Fatalf("idempotent retry: index=%d want=%d err=%v", index, e.Index, err)
	}
	if _, err := c.nodes[i].Publish(e.ID, []byte("conflicting payload"), time.Second); !errors.Is(err, replication.ErrConflict) {
		c.t.Fatalf("conflicting retry: got %v, want ErrConflict", err)
	}
}

func TestNodeThreeVoterFailoverAndConvergence(t *testing.T) {
	c := newKernelCluster(t)
	old := c.leader(-1)
	first := c.publish(old, "before-partition", "first")
	c.retry(old, first)
	want := []replication.Entry{first}
	c.converge(want, 0, 1, 2)

	// Removing one follower must not prevent the remaining majority committing.
	lagging := (old + 1) % 3
	other := (old + 2) % 3
	c.isolate(lagging)
	second := c.publish(old, "majority-only", "second")
	if second.Index <= first.Index {
		t.Fatal("committed indices did not increase")
	}
	want = append(want, second)
	c.converge(want, old, other)
	got, err := c.nodes[lagging].Entries(0, 1024)
	if err != nil || !reflect.DeepEqual(got, want[:1]) {
		t.Fatalf("isolated follower changed: entries=%v err=%v", got, err)
	}
	c.connect()
	c.converge(want, 0, 1, 2)

	c.isolate(old)
	// Call immediately, while the old leader may still believe it is leader.
	start := time.Now()
	if index, err := c.nodes[old].Publish("minority-only", []byte("must not commit"), 300*time.Millisecond); err == nil {
		t.Fatalf("minority reported successful commit at index %d", index)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("minority publish exceeded its bounded timeout")
	}
	next := c.leader(old)
	if next == old {
		t.Fatal("isolated leader was reselected")
	}
	c.retry(next, first)
	third := c.publish(next, "after-failover", "third")
	if third.Index <= second.Index {
		t.Fatal("indices did not increase after failover")
	}
	want = append(want, third)
	c.converge(want, lagging, other)
	if _, err := c.nodes[old].Publish("minority-again", []byte("still isolated"), 300*time.Millisecond); err == nil {
		t.Fatal("isolated node accepted a second publication")
	}
	c.connect()
	leader := c.leader(-1)
	c.converge(want, 0, 1, 2)
	c.retry(leader, first)
	fourth := c.publish(leader, "after-recovery", "fourth")
	want = append(want, fourth)
	c.converge(want, 0, 1, 2)
}

func TestNodeThreeVoterPersistentReopen(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(fmt.Sprintf("snapshot=%t", snapshot), func(t *testing.T) {
			c := newKernelCluster(t)
			leader := c.leader(-1)
			first := c.publish(leader, "durable-first", "first payload")
			want := []replication.Entry{first}
			c.converge(want, 0, 1, 2)
			if snapshot {
				for i, n := range c.nodes {
					if err := n.Snapshot(); err != nil {
						t.Fatalf("snapshot node %d: %v", i, err)
					}
				}
			}
			// This entry must survive as a WAL tail after the optional snapshot.
			second := c.publish(leader, "durable-tail", "second payload")
			want = append(want, second)
			c.converge(want, 0, 1, 2)
			c.closeNodes()
			for i, transport := range c.transports {
				if err := transport.Close(); err != nil {
					t.Fatalf("close old transport %d: %v", i, err)
				}
				_, c.transports[i] = raft.NewInmemTransportWithTimeout(raft.ServerAddress(c.configs[i].ID), time.Second)
				c.configs[i].Transport = c.transports[i]
				c.configs[i].Bootstrap = nil
			}
			c.connect()
			for i := range c.nodes {
				c.open(i)
			}
			leader = c.leader(-1)
			c.converge(want, 0, 1, 2)
			c.retry(leader, first)
			c.retry(leader, second)
			third := c.publish(leader, "after-reopen", "third payload")
			if third.Index <= second.Index {
				t.Fatal("index regressed after reopen")
			}
			want = append(want, third)
			c.converge(want, 0, 1, 2)
		})
	}
}
