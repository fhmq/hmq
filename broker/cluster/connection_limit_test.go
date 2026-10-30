package cluster

import (
	"net"
	"testing"
	"time"
)

func TestConnectionLimitAndRecovery(t *testing.T) {
	s, c := testTLS(t)
	n, err := Start(Config{NodeID: "a", ClusterID: "g", Secret: []byte("s"), ServerTLS: s, ClientTLS: c, HandshakeTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	var ends []net.Conn
	for i := 0; i < maxConnections; i++ {
		a, b := net.Pipe()
		ends = append(ends, b)
		n.launch(a, true)
	}
	defer func() {
		for _, c := range ends {
			c.Close()
		}
	}()
	a, b := net.Pipe()
	defer b.Close()
	if id := <-n.launch(a, true); id != "" {
		t.Fatalf("unexpected peer %q", id)
	}
	if _, err := b.Read(make([]byte, 1)); err == nil {
		t.Fatal("excess connection remained open")
	}
	ends[0].Close()
	deadline := time.Now().Add(time.Second)
	for {
		n.mu.Lock()
		count := len(n.conns)
		n.mu.Unlock()
		if count < maxConnections {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("connection slot not released")
		}
		time.Sleep(time.Millisecond)
	}
	a, b = net.Pipe()
	defer b.Close()
	n.launch(a, true)
	n.mu.Lock()
	count := len(n.conns)
	n.mu.Unlock()
	if count != maxConnections {
		t.Fatalf("connection not admitted: %d", count)
	}
}
