package cluster

import (
	"net"
	"testing"
	"time"
)

func TestIncomingHandshakeLimitAndRecovery(t *testing.T) {
	s, c := testTLS(t)
	n, err := Start(Config{NodeID: "a", ClusterID: "g", Secret: []byte("s"), ServerTLS: s, ClientTLS: c, HandshakeTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close()
	for i := 0; i < maxIncomingHandshakes; i++ {
		n.handshakeSlots <- struct{}{}
	}
	conn, err := net.Dial("tcp", n.Addr())
	if err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := conn.Read(b[:]); err == nil {
		t.Fatal("over-limit connection was not closed")
	} else if e, ok := err.(net.Error); ok && e.Timeout() {
		t.Fatal("over-limit connection was not rejected")
	}
	conn.Close()
	for i := 0; i < maxIncomingHandshakes; i++ {
		<-n.handshakeSlots
	}
	conn, err = net.Dial("tcp", n.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	deadline := time.Now().Add(time.Second)
	for len(n.handshakeSlots) != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(n.handshakeSlots) != 1 {
		t.Fatal("handshake did not acquire slot")
	}
	deadline = time.Now().Add(2 * time.Second)
	for len(n.handshakeSlots) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(n.handshakeSlots) != 0 {
		t.Fatal("timed-out handshake did not release slot")
	}
}
