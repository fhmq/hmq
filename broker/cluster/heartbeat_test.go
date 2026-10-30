package cluster

import (
	"bytes"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueueFullDisconnects(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	n := &Node{cfg: Config{MaxFrameSize: 10, QueueSize: 1, HeartbeatTimeout: time.Second, OnMessage: func(string, []byte) { close(started); <-release }}, done: make(chan struct{}), callbackSlots: make(chan struct{}, 1)}
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	done := make(chan struct{})
	go func() { n.receivePeer("peer", a, a); close(done) }()
	frame := []byte{0, 0, 0, 1, 'x'}
	if _, err := b.Write(frame); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("callback did not start")
	}
	go b.Write(bytes.Repeat(frame, 2))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("full queue did not disconnect")
	}
	close(release)
	n.callbackWG.Wait()
}

func TestHeartbeatSlowCallbackAndClose(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	s, c := configs(t, "a")
	a, err := Start(Config{NodeID: "a", ClusterID: "g", Secret: []byte("s"), ServerTLS: s, ClientTLS: c, HeartbeatInterval: 20 * time.Millisecond, HeartbeatTimeout: 150 * time.Millisecond, OnMessage: func(string, []byte) { close(started); <-release }})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	s, c = configs(t, "b")
	b, err := Start(Config{NodeID: "b", ClusterID: "g", Secret: []byte("s"), ServerTLS: s, ClientTLS: c, Seeds: []string{a.Addr()}, RetryInterval: 20 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, HeartbeatTimeout: 150 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	await(t, b, "a")
	b.Broadcast([]byte("x"))
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("callback did not start")
	}
	time.Sleep(400 * time.Millisecond)
	if len(a.Peers()) != 1 || len(b.Peers()) != 1 {
		t.Fatal("slow callback disconnected healthy peer")
	}
	done := make(chan struct{})
	go func() { a.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("close blocked")
	}
	close(release)
	a.WaitCallbacks()
}

func TestHeartbeatBlackholeReconnect(t *testing.T) {
	s, c := configs(t, "a")
	a, err := Start(Config{NodeID: "a", ClusterID: "g", Secret: []byte("s"), ServerTLS: s, ClientTLS: c, HeartbeatInterval: 20 * time.Millisecond, HeartbeatTimeout: 150 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var blackhole atomic.Bool
	var count atomic.Int32
	go func() {
		for {
			client, err := l.Accept()
			if err != nil {
				return
			}
			count.Add(1)
			upstream, err := net.Dial("tcp", a.Addr())
			if err != nil {
				client.Close()
				continue
			}
			go func() {
				defer client.Close()
				defer upstream.Close()
				go func() { io.Copy(upstream, client); upstream.Close() }()
				buf := make([]byte, 4096)
				for {
					n, err := upstream.Read(buf)
					if n > 0 && !blackhole.Load() {
						if _, e := client.Write(buf[:n]); e != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	s, c = configs(t, "b")
	b, err := Start(Config{NodeID: "b", ClusterID: "g", Secret: []byte("s"), ServerTLS: s, ClientTLS: c, Seeds: []string{l.Addr().String()}, RetryInterval: 20 * time.Millisecond, HandshakeTimeout: 200 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond, HeartbeatTimeout: 150 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	await(t, b, "a")
	blackhole.Store(true)
	deadline := time.Now().Add(2 * time.Second)
	for count.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if count.Load() < 2 {
		t.Fatal("blackhole did not trigger reconnect")
	}
	blackhole.Store(false)
	await(t, b, "a")
}
