package cluster

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestStatusSnapshot(t *testing.T) {
	n := &Node{cfg: Config{Secret: []byte("private-secret")}, peers: map[string]*peer{"z": {outgoing: true}, "a": {}}}
	s := n.Status()
	if s.State != "running" || s.ErrorsAvailable || len(s.Peers) != 2 || s.Peers[0].ID != "a" || s.Peers[0].Direction != "incoming" || s.Peers[1].Direction != "outgoing" {
		t.Fatalf("unexpected status: %+v", s)
	}
	s.Peers[0].ID = "changed"
	if n.Status().Peers[0].ID != "a" {
		t.Fatal("snapshot aliases node state")
	}
	b, err := json.Marshal(n.Status())
	if err != nil || strings.Contains(string(b), "private-secret") {
		t.Fatalf("unsafe status: %s, %v", b, err)
	}
	n.closed = true
	if n.Status().State != "closed" {
		t.Fatal("closed state missing")
	}
}
