package broker

import (
	"bytes"
	"errors"
	"net"
	"testing"
	"time"

	mqtt5 "github.com/eclipse/paho.golang/packets"
	"github.com/fhmq/hmq/v4/broker/packets"
)

func TestSlowConsumerDoesNotDisconnectPublisher(t *testing.T) {
	addr := mqtt5ITBroker(t, nil)
	slow := mqtt5ITClient(t, addr, "slow", func(p *mqtt5.Connect) { n := uint16(1); p.Properties.ReceiveMaximum = &n })
	mqtt5ITSubscribe(t, slow, 1, mqtt5.SubOptions{Topic: "slow/test", QoS: 1}, nil)
	pub := mqtt5ITClient(t, addr, "publisher", nil)
	for i := 1; i <= 260; i++ {
		mqtt5ITPublish(t, pub, &mqtt5.Publish{Topic: "slow/test", QoS: 1, PacketID: uint16(i), Payload: []byte("x")})
	}
	mqtt5ITPing(t, pub)
	_ = slow.SetReadDeadline(time.Now().Add(time.Second))
	for {
		if _, err := mqtt5.ReadPacket(slow); err != nil {
			if n, ok := err.(net.Error); ok && n.Timeout() {
				t.Fatal("slow consumer was not disconnected")
			}
			break
		}
	}
}

func TestSharedOversizeTriesNextConsumer(t *testing.T) {
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Payload = "test", bytes.Repeat([]byte("x"), 64)
	small := &client{mqtt5: &mqtt5State{maximumPacket: 8}}
	server, peer := net.Pipe()
	defer server.Close()
	defer peer.Close()
	healthy := &client{mqtt5: &mqtt5State{maximumPacket: 1024}, status: Connected, conn: server}
	read := make(chan error, 1)
	go func() { _, err := mqtt5.ReadPacket(peer); read <- err }()
	clients := []*client{small, healthy}
	attempts := 0
	err := deliverSharedCandidates([]int{0, 1}, 0, func(i int) (bool, error) {
		attempts++
		err := clients[i].sendPublication(p, true, false)
		if errors.Is(err, errMQTT5DeliveryTooLarge) {
			return false, nil
		}
		return err == nil, err
	})
	if err != nil || attempts != 2 {
		t.Fatalf("attempts=%d error=%v", attempts, err)
	}
	if err := <-read; err != nil {
		t.Fatal(err)
	}
}

func TestMQTT5SizePreflightDoesNotCopyPayload(t *testing.T) {
	p := &mqtt5.Publish{Topic: "test", Payload: make([]byte, 1<<20), Properties: &mqtt5.Properties{}}
	state := &mqtt5State{maximumPacket: 2 << 20}
	result := testing.Benchmark(func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if _, err := mqtt5PacketSize(state, p); err != nil {
				b.Fatal(err)
			}
		}
	})
	if result.AllocedBytesPerOp() > 4096 {
		t.Fatalf("preflight allocates %d bytes", result.AllocedBytesPerOp())
	}
}

type admissionConn struct {
	net.Conn
	address net.Addr
}

func (c admissionConn) RemoteAddr() net.Addr { return c.address }

func TestConnectionAdmissionLimitsAndRelease(t *testing.T) {
	b := &Broker{config: &Config{MaxConnections: 2, MaxConnectionsPerIP: 1}, stopped: make(chan struct{}), connections: make(map[net.Conn]struct{})}
	makeConn := func(ip string) net.Conn {
		a, z := net.Pipe()
		t.Cleanup(func() { a.Close(); z.Close() })
		return admissionConn{a, &net.TCPAddr{IP: net.ParseIP(ip), Port: 1234}}
	}
	a := makeConn("127.0.0.1")
	if !b.registerConnection(a) {
		t.Fatal("first rejected")
	}
	if b.registerConnection(makeConn("127.0.0.1")) {
		t.Fatal("per-IP limit bypassed")
	}
	z := makeConn("127.0.0.2")
	if !b.registerConnection(z) {
		t.Fatal("second IP rejected")
	}
	if b.registerConnection(makeConn("127.0.0.3")) {
		t.Fatal("global limit bypassed")
	}
	b.unregisterConnection(a)
	b.unregisterConnection(z)
	next := makeConn("127.0.0.1")
	if !b.registerConnection(next) {
		t.Fatal("slot not released")
	}
	b.unregisterConnection(next)
	if len(b.connectionsPerIP) != 0 {
		t.Fatal("IP counters leaked")
	}
}

func TestConnectReadDeadlineAndClear(t *testing.T) {
	addr := mqtt5ITBroker(t, &Config{ConnectTimeoutSeconds: 1})
	idle := mqtt5ITDial(t, addr)
	if _, err := idle.Write([]byte{0x10}); err != nil {
		t.Fatal(err)
	}
	_ = idle.SetReadDeadline(time.Now().Add(3 * time.Second))
	var b [1]byte
	if _, err := idle.Read(b[:]); err == nil {
		t.Fatal("incomplete CONNECT accepted")
	} else if n, ok := err.(net.Error); ok && n.Timeout() {
		t.Fatal("CONNECT deadline not enforced")
	}
	active := mqtt5ITClient(t, addr, "active", nil)
	time.Sleep(1100 * time.Millisecond)
	mqtt5ITPing(t, active)
}

func TestConnectionLimitsRejectNegativeConfiguration(t *testing.T) {
	for _, config := range []Config{{ConnectTimeoutSeconds: -1}, {MaxConnections: -1}, {MaxConnectionsPerIP: -1}} {
		if b, err := NewBroker(&config); err == nil {
			b.Close()
			t.Fatal("negative limit accepted")
		}
	}
}
