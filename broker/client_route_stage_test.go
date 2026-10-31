package broker

import (
	"net"
	"testing"
	"time"

	"github.com/fhmq/hmq/v4/broker/packets"
)

type blockingRouteWriteConn struct {
	net.Conn
	started chan struct{}
}

func (c *blockingRouteWriteConn) Write(p []byte) (int, error) {
	close(c.started)
	return c.Conn.Write(p)
}

func TestDetachInterruptsBlockedWrite(t *testing.T) {
	b, err := NewBroker(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	server, peer := net.Pipe()
	defer peer.Close()
	w := &blockingRouteWriteConn{Conn: server, started: make(chan struct{})}
	c := &client{broker: b, typ: CLIENT, conn: w, info: info{clientID: "blocked-writer"}}
	c.init()
	b.clients.Store(c.info.clientID, c)
	written := make(chan error, 1)
	go func() { written <- c.WriterPacket(packets.NewControlPacket(packets.Pingresp)) }()
	select {
	case <-w.started:
	case <-time.After(time.Second):
		t.Fatal("write did not start")
	}
	detached := make(chan func(), 1)
	go func() {
		unlock := b.lockClientConnect(c.info.clientID)
		cleanup := c.detach()
		unlock()
		detached <- cleanup
	}()
	select {
	case cleanup := <-detached:
		if cleanup == nil {
			t.Fatal("missing cleanup")
		}
		cleanup()
	case <-time.After(time.Second):
		peer.Close()
		<-detached
		t.Fatal("detach blocked on an in-progress write")
	}
	select {
	case err := <-written:
		if err == nil {
			t.Fatal("blocked write unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("close did not interrupt write")
	}
}

func TestClientRouteIndependentAndDetachedClose(t *testing.T) {
	b, err := NewBroker(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	server, peer := net.Pipe()
	defer peer.Close()
	w := &windowConn{Conn: server, closeStarted: make(chan struct{}), closeRelease: make(chan struct{})}
	c := &client{broker: b, typ: CLIENT, conn: w, info: info{clientID: "route-a"}}
	c.init()
	b.clients.Store(c.info.clientID, c)

	unlock := b.lockClientConnect("route-a")
	independent := make(chan struct{})
	go func() { release := b.lockClientConnect("route-b"); release(); close(independent) }()
	select {
	case <-independent:
	case <-time.After(time.Second):
		t.Fatal("different client blocked")
	}
	cleanup := c.detach()
	if cleanup == nil {
		t.Fatal("missing cleanup")
	}
	replacement := &client{info: info{clientID: "route-a"}}
	b.clients.Store("route-a", replacement)
	unlock()
	done := make(chan struct{})
	go func() { cleanup(); close(done) }()
	select {
	case <-w.closeStarted:
	case <-time.After(time.Second):
		t.Fatal("close did not start")
	}
	defer func() { close(w.closeRelease); <-done }()
	routed := make(chan error, 1)
	go func() {
		routed <- b.withClientRoute("route-a", func(got *client) error {
			if got != replacement {
				t.Errorf("routed to stale client")
			}
			return nil
		})
	}()
	select {
	case err := <-routed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("route blocked by physical close")
	}
	b.clients.Delete("route-a")
}
