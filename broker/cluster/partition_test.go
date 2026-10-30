package cluster

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// faultLink forwards TCP traffic until partitioned, including existing streams.
type faultLink struct {
	listener net.Listener
	target   string
	mu       sync.Mutex
	blocked  bool
	conns    map[net.Conn]bool
	wg       sync.WaitGroup
}

func newFaultLink(t *testing.T, target string) *faultLink {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &faultLink{listener: l, target: target, conns: make(map[net.Conn]bool)}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			if f.blocked {
				f.mu.Unlock()
				c.Close()
				continue
			}
			f.conns[c] = true
			f.wg.Add(1)
			f.mu.Unlock()
			go func() {
				defer f.wg.Done()
				defer func() { f.mu.Lock(); delete(f.conns, c); f.mu.Unlock(); c.Close() }()
				u, err := net.DialTimeout("tcp", f.target, time.Second)
				if err != nil {
					return
				}
				defer u.Close()
				go func() { io.Copy(u, c); u.Close() }()
				io.Copy(c, u)
			}()
		}
	}()
	t.Cleanup(func() { f.setBlocked(true); l.Close(); f.wg.Wait() })
	return f
}

func (f *faultLink) setBlocked(blocked bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocked = blocked
	if blocked {
		for c := range f.conns {
			c.Close()
		}
	}
}

func TestThreeNodePartitionAndRecovery(t *testing.T) {
	messages := make(chan string, 16)
	makeNode := func(id string, seeds ...string) *Node {
		s, c := configs(t, id)
		n, err := Start(Config{NodeID: id, ClusterID: "g", Secret: []byte("s"), ServerTLS: s, ClientTLS: c, Seeds: seeds, RetryInterval: 20 * time.Millisecond, HandshakeTimeout: 200 * time.Millisecond, OnMessage: func(peer string, p []byte) { messages <- id + ":" + peer + ":" + string(p) }})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { n.Close(); n.WaitCallbacks() })
		return n
	}
	a := makeNode("a")
	ab := newFaultLink(t, a.Addr())
	b := makeNode("b", ab.listener.Addr().String())
	ac := newFaultLink(t, a.Addr())
	bc := newFaultLink(t, b.Addr())
	c := makeNode("c", ac.listener.Addr().String(), bc.listener.Addr().String())
	await(t, a, "b", "c")
	await(t, b, "a", "c")
	await(t, c, "a", "b")
	ac.setBlocked(true)
	bc.setBlocked(true)
	await(t, a, "b")
	await(t, b, "a")
	deadline := time.Now().Add(3 * time.Second)
	for len(c.Peers()) != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(c.Peers()) != 0 {
		t.Fatalf("isolated node peers: %v", c.Peers())
	}
	if errs := a.Broadcast([]byte("during")); len(errs) != 0 {
		t.Fatal(errs)
	}
	select {
	case got := <-messages:
		if got != "b:a:during" {
			t.Fatal(got)
		}
	case <-time.After(time.Second):
		t.Fatal("connected nodes failed to communicate")
	}
	ac.setBlocked(false)
	bc.setBlocked(false)
	await(t, a, "b", "c")
	await(t, b, "a", "c")
	await(t, c, "a", "b")
	if errs := c.Broadcast([]byte("after")); len(errs) != 0 {
		t.Fatal(errs)
	}
	got := map[string]bool{}
	for len(got) < 2 {
		select {
		case m := <-messages:
			got[m] = true
		case <-time.After(time.Second):
			t.Fatalf("messages after recovery: %v", got)
		}
	}
	if !got["a:c:after"] || !got["b:c:after"] {
		t.Fatalf("unexpected messages: %v", got)
	}
}
