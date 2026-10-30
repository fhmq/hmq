package broker

import (
	"bytes"
	stdjson "encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReliableClusterRejectsPeerMembershipMismatch(t *testing.T) {
	h := newReliableClusterHarness(t).(*realReliableCluster)
	h.clients["c"].Disconnect(100)
	h.nodes["c"].Close()
	delete(h.clients, "c")
	delete(h.nodes, "c")
	cfg := h.configs["c"]
	cfg.Cluster.Reliable.MemberIDs = []string{"a", "c", "d"}
	dir := t.TempDir()
	cfg.Cluster.Reliable.EventLogPath = filepath.Join(dir, "events")
	cfg.Cluster.Reliable.InboxLogPath = filepath.Join(dir, "inbox")
	h.configs["c"] = cfg
	h.start("c")
	// Allow several connection attempts, then check both directions.
	time.Sleep(3 * time.Second)
	if len(h.nodes["c"].clusterNode.Peers()) != 0 {
		t.Fatalf("mismatched node connected: %v", h.nodes["c"].clusterNode.Peers())
	}
	for _, id := range []string{"a", "b"} {
		for _, peer := range h.nodes[id].clusterNode.Peers() {
			if peer == "c" {
				t.Fatalf("%s connected to mismatched c", id)
			}
		}
	}
	h.Publish("a", "membership-mismatch")
	reliableExpectDelivery(t, h.Deliveries("b"), "membership-mismatch")
	reliableExpectQuiet(t, h.Deliveries("c"))
	pending, err := h.nodes["a"].reliableRuntime.events.Pending("c", 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, event := range pending {
		var publication clusterPublish
		if stdjson.Unmarshal(event.Payload, &publication) == nil && string(publication.Payload) == "membership-mismatch" {
			found = true
		}
	}
	if !found {
		t.Fatal("publication was not pending for mismatched c")
	}
}

func TestReliableClusterRejectsChangedMembershipOnRestart(t *testing.T) {
	h := newReliableClusterHarness(t).(*realReliableCluster)
	h.clients["c"].Disconnect(100)
	h.nodes["c"].Close()
	delete(h.clients, "c")
	delete(h.nodes, "c")
	cfg := h.configs["c"]
	cfg.Cluster.Reliable.MemberIDs = []string{"a", "c", "d"}
	b, err := NewBroker(&cfg)
	if err == nil {
		b.Close()
		t.Fatal("restart with different fixed membership succeeded")
	}
	h.start("c")
	h.Publish("a", "membership-restored")
	reliableExpectDelivery(t, h.Deliveries("c"), "membership-restored")
}

func TestReliableClusterRejectsMismatchedLogPair(t *testing.T) {
	h := newReliableClusterHarness(t).(*realReliableCluster)
	for _, id := range []string{"b", "c"} {
		h.clients[id].Disconnect(100)
		h.nodes[id].Close()
		delete(h.clients, id)
		delete(h.nodes, id)
	}
	cfg := h.configs["c"]
	eventPath := cfg.Cluster.Reliable.EventLogPath
	before, err := os.ReadFile(eventPath)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Cluster.Reliable.InboxLogPath = h.configs["b"].Cluster.Reliable.InboxLogPath
	b, err := NewBroker(&cfg)
	if err == nil {
		b.Close()
		t.Fatal("restart with another node's inbox succeeded")
	}
	after, err := os.ReadFile(eventPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("rejected restart modified event log")
	}
	h.start("b")
	h.start("c")
	h.Publish("a", "pair-restored")
	reliableExpectDelivery(t, h.Deliveries("c"), "pair-restored")
}

func TestReliableClusterPartitionedReceiverRestart(t *testing.T) {
	h := newReliableClusterHarness(t)
	h.Partition("c", true)
	h.Publish("a", "restart-during-partition")
	reliableExpectDelivery(t, h.Deliveries("b"), "restart-during-partition")
	h.Restart("c")
	reliableExpectQuiet(t, h.Deliveries("c"))
	h.Partition("c", false)
	reliableExpectDelivery(t, h.Deliveries("c"), "restart-during-partition")
	reliableExpectQuiet(t, h.Deliveries("c"))
}
