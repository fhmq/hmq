package cluster

import (
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"
)

func TestCongestedPeerDoesNotDelayHealthyPeer(t *testing.T) {
	blocked, blackhole := net.Pipe()
	good, receiver := net.Pipe()
	defer blackhole.Close()
	defer receiver.Close()
	defer good.Close()
	n := &Node{cfg: Config{MaxFrameSize: 100, WriteTimeout: 500 * time.Millisecond}, broadcastSlots: make(chan struct{}, maxConcurrentBroadcasts), peers: map[string]*peer{
		"blocked": {conn: blocked}, "good": {conn: good},
	}}
	result := make(chan map[string]error, 1)
	go func() { result <- n.Broadcast([]byte("hello")) }()
	frame := make([]byte, 9)
	receiver.SetReadDeadline(time.Now().Add(250 * time.Millisecond))
	if _, err := io.ReadFull(receiver, frame); err != nil || string(frame[4:]) != "hello" {
		t.Fatalf("healthy peer did not receive frame promptly: %q, %v", frame, err)
	}
	select {
	case errs := <-result:
		if errs["blocked"] == nil || errs["good"] != nil {
			t.Fatalf("unexpected failures: %v", errs)
		}
		if !errors.Is(errs["blocked"], ErrPeerWrite) || !errors.Is(errs["blocked"], os.ErrDeadlineExceeded) {
			t.Fatalf("write failure lost its classification or cause: %v", errs["blocked"])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("broadcast did not time out")
	}
}

func TestWriteFrameQueueTimeoutAndClose(t *testing.T) {
	for _, closeNode := range []bool{false, true} {
		t.Run(map[bool]string{false: "timeout", true: "close"}[closeNode], func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			n := &Node{cfg: Config{WriteTimeout: 120 * time.Millisecond}, done: make(chan struct{})}
			p := &peer{conn: a, done: make(chan struct{})}
			p.writeOnce.Do(func() { p.writeSlot = make(chan struct{}, 1) })
			p.writeSlot <- struct{}{}
			const count = 200
			results := make(chan error, count)
			start := time.Now()
			for i := 0; i < count; i++ {
				go func() { results <- n.writeFrame(p, []byte{0, 0, 0, 1}, []byte("x")) }()
			}
			if closeNode {
				close(n.done)
			}
			for i := 0; i < count; i++ {
				select {
				case err := <-results:
					if closeNode && !errors.Is(err, net.ErrClosed) || !closeNode && (err == nil || !errors.Is(err, os.ErrDeadlineExceeded)) {
						t.Fatalf("unexpected error: %v", err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("queued writer did not return")
				}
			}
			if time.Since(start) > time.Second {
				t.Fatal("queued writers exceeded their bound")
			}
		})
	}
}

func TestWriteFrameAlreadyDisconnected(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	n := &Node{cfg: Config{WriteTimeout: time.Second}, done: make(chan struct{})}
	p := &peer{conn: a, done: make(chan struct{})}
	close(p.done)
	for i := 0; i < 100; i++ {
		if err := n.writeFrame(p, []byte{0, 0, 0, 1}, []byte("x")); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("write after disconnect: %v", err)
		}
	}
}

func TestBroadcastSaturationAndClose(t *testing.T) {
	s, c := configs(t, "a")
	n, err := Start(Config{NodeID: "a", ClusterID: "g", Secret: []byte("s"), ServerTLS: s, ClientTLS: c, WriteTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	a, b := net.Pipe()
	defer b.Close()
	n.mu.Lock()
	n.peers["blocked"] = &peer{conn: a}
	n.conns[a] = struct{}{}
	n.mu.Unlock()
	var wg sync.WaitGroup
	for i := 0; i < maxConcurrentBroadcasts; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); n.Broadcast([]byte("x")) }()
	}
	deadline := time.Now().Add(3 * time.Second)
	for len(n.broadcastSlots) != maxConcurrentBroadcasts && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(n.broadcastSlots) != maxConcurrentBroadcasts {
		n.Close()
		wg.Wait()
		t.Fatal("broadcasts did not saturate")
	}
	if err := n.Broadcast([]byte("overflow"))[""]; !errors.Is(err, ErrBroadcastBusy) {
		t.Fatalf("expected backpressure, got %v", err)
	}
	done := make(chan struct{})
	go func() { n.Close(); wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("close left broadcasts blocked")
	}
	if len(n.broadcastSlots) != 0 {
		t.Fatal("broadcast slots leaked")
	}
	if err := n.Broadcast(nil)[""]; !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closed broadcast: %v", err)
	}
}

func TestHalfFrameTimesOut(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	n := &Node{cfg: Config{MaxFrameSize: 100, QueueSize: 1, HeartbeatTimeout: 100 * time.Millisecond}, done: make(chan struct{}), callbackSlots: make(chan struct{}, 1)}
	done := make(chan struct{})
	go func() { n.receivePeer("peer", a, a); close(done) }()
	if _, err := b.Write([]byte{0, 0, 0, 5, 'x'}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("incomplete frame did not time out")
	}
}
