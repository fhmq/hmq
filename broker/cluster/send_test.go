package cluster

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestSendPartitionReconnect(t *testing.T) {
	received := make(chan string, 4)
	s, c := configs(t, "a")
	a, err := Start(Config{NodeID: "a", ClusterID: "g", Secret: []byte("s"), ServerTLS: s, ClientTLS: c, OnMessage: func(id string, p []byte) { received <- id + ":" + string(p) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close(); a.WaitCallbacks() })
	link := newFaultLink(t, a.Addr())
	s, c = configs(t, "b")
	b, err := Start(Config{NodeID: "b", ClusterID: "g", Secret: []byte("s"), ServerTLS: s, ClientTLS: c, Seeds: []string{link.listener.Addr().String()}, RetryInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close(); b.WaitCallbacks() })
	await(t, b, "a")
	if err := b.Send("a", []byte("first")); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if got != "b:first" {
			t.Fatal(got)
		}
	case <-time.After(time.Second):
		t.Fatal("no message")
	}
	link.setBlocked(true)
	deadline := time.Now().Add(3 * time.Second)
	for len(b.Peers()) != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(b.Peers()) != 0 {
		t.Fatal("peer did not disconnect")
	}
	if err := b.Send("a", nil); !errors.Is(err, ErrPeerNotConnected) {
		t.Fatalf("disconnected: %v", err)
	}
	link.setBlocked(false)
	await(t, b, "a")
	if err := b.Send("a", []byte("again")); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-received:
		if got != "b:again" {
			t.Fatal(got)
		}
	case <-time.After(time.Second):
		t.Fatal("no message after reconnect")
	}
}

func TestSendConcurrentClose(t *testing.T) {
	a := start(t, "a", "g", "s")
	b := start(t, "b", "g", "s", a.Addr())
	await(t, b, "a")
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = b.Send("a", []byte("x")) }()
	}
	b.Close()
	wg.Wait()
	if err := b.Send("a", nil); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
}

func TestSendWriteFailureAndConcurrentClose(t *testing.T) {
	a, b := net.Pipe()
	n := &Node{peers: map[string]*peer{"peer": {conn: a, done: make(chan struct{})}}, done: make(chan struct{}), broadcastSlots: make(chan struct{}, maxConcurrentBroadcasts), cfg: Config{MaxFrameSize: 1024, WriteTimeout: time.Second}}
	b.Close()
	if err := n.Send("peer", []byte("x")); !errors.Is(err, ErrPeerWrite) {
		t.Fatalf("write failure: %v", err)
	}
	close(n.done)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = n.Send("peer", nil) }()
	}
	wg.Wait()
}
