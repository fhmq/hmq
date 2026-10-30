package replication

import (
	"errors"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestNodeDirectoryIdentity(t *testing.T) {
	dir := t.TempDir()
	_, transport := raft.NewInmemTransport("address")
	defer transport.Close()
	n, err := Open(Config{ID: "one", Dir: dir, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := n.Publish("after-close", nil, time.Second); !errors.Is(err, raft.ErrRaftShutdown) {
		t.Fatalf("closed publish: %v", err)
	}
	if n, err := Open(Config{ID: "two", Dir: dir, Transport: transport}); !errors.Is(err, ErrIdentity) {
		if n != nil {
			n.Close()
		}
		t.Fatalf("directory reassigned: %v", err)
	}
	if n, err := Open(Config{ID: "one", ClusterID: "another", Dir: dir, Transport: transport}); !errors.Is(err, ErrIdentity) {
		if n != nil {
			n.Close()
		}
		t.Fatalf("directory moved to another cluster: %v", err)
	}
	n, err = Open(Config{ID: "one", Dir: dir, Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
}
