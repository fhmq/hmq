package replication

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

type tlsStreamPKI struct {
	ca    *x509.Certificate
	key   *ecdsa.PrivateKey
	roots *x509.CertPool
}

func newTLSStreamPKI(t *testing.T) *tlsStreamPKI {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "stream test CA"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	return &tlsStreamPKI{ca, key, roots}
}

func (p *tlsStreamPKI) configs(t *testing.T, ids ...string) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: serial, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	for _, id := range ids {
		u, err := url.Parse("urn:hmq:node:" + id)
		if err != nil {
			t.Fatal(err)
		}
		cert.URIs = append(cert.URIs, u)
	}
	der, err := x509.CreateCertificate(rand.Reader, cert, p.ca, &key.PublicKey, p.key)
	if err != nil {
		t.Fatal(err)
	}
	pair := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	return &tls.Config{Certificates: []tls.Certificate{pair}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: p.roots, MinVersion: tls.VersionTLS12}, &tls.Config{Certificates: []tls.Certificate{pair}, RootCAs: p.roots, MinVersion: tls.VersionTLS12}
}

func tlsStreamListeners(t *testing.T, ids ...string) (map[string]net.Listener, map[string]string) {
	t.Helper()
	ls, peers := make(map[string]net.Listener), make(map[string]string)
	for _, id := range ids {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ls[id], peers[id] = l, l.Addr().String()
		// Also covers construction failures, when ownership has not transferred.
		t.Cleanup(func() { _ = l.Close() })
	}
	return ls, peers
}

func startTLSStream(t *testing.T, p *tlsStreamPKI, id, cluster string, peers map[string]string, l net.Listener, timeout time.Duration) *TLSStream {
	t.Helper()
	server, client := p.configs(t, id)
	s, err := ListenTLS(TLSStreamConfig{ID: id, ClusterID: cluster, Peers: peers, Listener: l, ServerTLS: server, ClientTLS: client, Timeout: timeout})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if s.Addr().String() != peers[id] {
		t.Fatalf("advertised address = %s", s.Addr())
	}
	return s
}

func TestTLSStreamMutualTLS(t *testing.T) {
	p := newTLSStreamPKI(t)
	ls, peers := tlsStreamListeners(t, "a", "b")
	a := startTLSStream(t, p, "a", "cluster", peers, ls["a"], time.Second)
	b := startTLSStream(t, p, "b", "cluster", peers, ls["b"], time.Second)
	for _, direction := range []struct {
		from, to *TLSStream
		id       string
	}{{a, b, "b"}, {b, a, "a"}} {
		out, err := direction.from.Dial(raft.ServerAddress(peers[direction.id]), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer out.Close()
		accepted := make(chan net.Conn, 1)
		go func() { c, _ := direction.to.Accept(); accepted <- c }()
		var in net.Conn
		select {
		case in = <-accepted:
		case <-time.After(2 * time.Second):
			t.Fatal("Accept hung")
		}
		if in == nil {
			t.Fatal("Accept failed")
		}
		defer in.Close()
		for _, c := range []net.Conn{out, in} {
			state := c.(*streamConn).Conn.(*tls.Conn).ConnectionState()
			if !state.HandshakeComplete || len(state.VerifiedChains) == 0 {
				t.Fatal("connection is not verified TLS")
			}
			_ = c.SetDeadline(time.Now().Add(time.Second))
		}
		for _, pair := range [][2]net.Conn{{out, in}, {in, out}} {
			payload := bytes.Repeat([]byte("bidirectional stream\x00"), 8192)
			written := make(chan error, 1)
			go func() { _, err := io.Copy(pair[0], bytes.NewReader(payload)); written <- err }()
			got := make([]byte, len(payload))
			if _, err := io.ReadFull(pair[1], got); err != nil {
				t.Fatal(err)
			}
			if err := <-written; err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, payload) {
				t.Fatal("stream payload differs")
			}
		}
	}
}

func TestTLSStreamRejectsConfigurationMismatch(t *testing.T) {
	for _, kind := range []string{"cluster", "membership", "server-uri"} {
		t.Run(kind, func(t *testing.T) {
			p := newTLSStreamPKI(t)
			ls, peers := tlsStreamListeners(t, "a", "b", "c")
			a := startTLSStream(t, p, "a", "cluster", peers, ls["a"], time.Second)
			other := make(map[string]string)
			for k, v := range peers {
				other[k] = v
			}
			cluster, id := "cluster", "b"
			if kind == "cluster" {
				cluster = "other"
			}
			if kind == "membership" {
				delete(other, "c")
			}
			if kind == "server-uri" {
				id = "c"
				other["b"], other["c"] = other["c"], other["b"]
			}
			startTLSStream(t, p, id, cluster, other, ls["b"], time.Second)
			c, err := a.Dial(raft.ServerAddress(peers["b"]), time.Second)
			if c != nil {
				c.Close()
			}
			if err == nil {
				t.Fatal("accepted inconsistent peer")
			}
		})
	}
}

func TestTLSStreamRejectsClientIdentity(t *testing.T) {
	for _, kind := range []string{"nonmember", "spoofed-hello", "multiple-uri", "missing-uri", "untrusted-ca", "no-certificate", "self", "wrong-protocol", "missing-protocol"} {
		t.Run(kind, func(t *testing.T) {
			p := newTLSStreamPKI(t)
			ls, peers := tlsStreamListeners(t, "a", "b")
			s := startTLSStream(t, p, "a", "cluster", peers, ls["a"], 300*time.Millisecond)
			ids := []string{"b"}
			if kind == "self" {
				ids = []string{"a"}
			}
			if kind == "nonmember" {
				ids = []string{"outsider"}
			}
			if kind == "multiple-uri" {
				ids = []string{"b", "a"}
			}
			if kind == "missing-uri" {
				ids = nil
			}
			issuer := p
			if kind == "untrusted-ca" {
				issuer = newTLSStreamPKI(t)
			}
			_, config := issuer.configs(t, ids...)
			config.RootCAs = p.roots
			config.ServerName = "127.0.0.1"
			if kind == "no-certificate" {
				config.Certificates = nil
			}
			raw, err := net.DialTimeout("tcp", peers["a"], time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			c := tls.Client(raw, config)
			_ = c.SetDeadline(time.Now().Add(time.Second))
			if err = c.Handshake(); err == nil {
				hello := s.hello
				hello.ID = "b"
				if kind == "self" {
					hello.ID = "a"
				}
				if kind == "wrong-protocol" {
					hello.Protocol = "mqtt"
				}
				if kind == "missing-protocol" {
					hello.Protocol = ""
				}
				if kind == "spoofed-hello" {
					hello.ID = "a"
				}
				if err = writeStreamHello(c, hello); err == nil {
					_, err = readStreamHello(c)
				}
			}
			if err == nil {
				t.Fatal("unauthorized client received authenticated hello")
			}
			if e, ok := err.(net.Error); ok && e.Timeout() {
				t.Fatalf("peer timed out instead of rejecting identity: %v", err)
			}
			accepted := make(chan error, 1)
			go func() {
				c, e := s.Accept()
				if c != nil {
					c.Close()
				}
				accepted <- e
			}()
			select {
			case <-accepted:
				t.Fatal("unauthorized connection reached Accept")
			case <-time.After(20 * time.Millisecond):
			}
			_ = s.Close()
			select {
			case e := <-accepted:
				if !errors.Is(e, net.ErrClosed) {
					t.Fatalf("Accept error: %v", e)
				}
			case <-time.After(time.Second):
				t.Fatal("Accept did not stop")
			}
		})
	}
}

func TestTLSStreamTimeoutAndClose(t *testing.T) {
	p := newTLSStreamPKI(t)
	ls, peers := tlsStreamListeners(t, "a", "b")
	s := startTLSStream(t, p, "a", "cluster", peers, ls["a"], 150*time.Millisecond)
	raw, err := net.Dial("tcp", peers["a"])
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	_ = raw.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err = raw.Read(make([]byte, 1)); err == nil {
		t.Fatal("idle handshake was not closed")
	}
	if e, ok := err.(net.Error); ok && e.Timeout() {
		t.Fatal("server handshake timeout did not fire")
	}
	// The bound peer accepts TCP but never speaks TLS.
	done := make(chan error, 1)
	go func() {
		c, e := s.Dial(raft.ServerAddress(peers["b"]), time.Second)
		if c != nil {
			c.Close()
		}
		done <- e
	}()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("stalled Dial succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("Dial exceeded configured timeout")
	}
	for i := 0; i < 4; i++ {
		c, e := net.Dial("tcp", peers["a"])
		if e != nil {
			t.Fatal(e)
		}
		defer c.Close()
	}
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case e := <-closed:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("Close hung on TLS handshake")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	if c, e := s.Accept(); c != nil || !errors.Is(e, net.ErrClosed) {
		t.Fatalf("Accept after Close: %v, %v", c, e)
	}
	if c, e := s.Dial(raft.ServerAddress(peers["b"]), time.Second); c != nil || !errors.Is(e, net.ErrClosed) {
		t.Fatalf("Dial after Close: %v, %v", c, e)
	}
	if c, e := net.DialTimeout("tcp", peers["a"], 100*time.Millisecond); e == nil {
		c.Close()
		t.Fatal("Close did not close owned listener")
	}
}

func TestTLSStreamNetworkTransportReplicationAndSnapshot(t *testing.T) {
	p := newTLSStreamPKI(t)
	ls, peers := tlsStreamListeners(t, "a", "b", "c")
	transports := make(map[string]*raft.NetworkTransport)
	for _, id := range []string{"a", "b", "c"} {
		s := startTLSStream(t, p, id, "cluster", peers, ls[id], time.Second)
		n := raft.NewNetworkTransport(s, 2, 2*time.Second, io.Discard)
		transports[id] = n
		t.Cleanup(func() { _ = n.Close() })
	}
	// Exercise every directed edge, including pooled connections reused for logs.
	for _, from := range []string{"a", "b", "c"} {
		for _, to := range []string{"a", "b", "c"} {
			if from == to {
				continue
			}
			for index := uint64(1); index <= 2; index++ {
				payload := []byte("log " + from + " -> " + to)
				result := make(chan error, 1)
				go func() {
					var resp raft.AppendEntriesResponse
					e := transports[from].AppendEntries(raft.ServerID(to), raft.ServerAddress(peers[to]), &raft.AppendEntriesRequest{Term: 1, Entries: []*raft.Log{{Index: index, Term: 1, Type: raft.LogCommand, Data: payload}}}, &resp)
					if e == nil && (!resp.Success || resp.LastLog != index) {
						e = errors.New("incorrect append response")
					}
					result <- e
				}()
				select {
				case rpc := <-transports[to].Consumer():
					req, ok := rpc.Command.(*raft.AppendEntriesRequest)
					if !ok || len(req.Entries) != 1 || req.Entries[0].Index != index || !bytes.Equal(req.Entries[0].Data, payload) {
						rpc.Respond(nil, errors.New("bad append"))
						t.Fatal("incorrect replicated log")
					}
					rpc.Respond(&raft.AppendEntriesResponse{Term: 1, LastLog: index, Success: true}, nil)
				case <-time.After(3 * time.Second):
					t.Fatal("append RPC not received")
				}
				select {
				case e := <-result:
					if e != nil {
						t.Fatal(e)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("append response hung")
				}
			}
		}
	}
	for _, to := range []string{"b", "c"} {
		payload := bytes.Repeat([]byte("snapshot\x00"), 131072)
		result := make(chan error, 1)
		go func() {
			var resp raft.InstallSnapshotResponse
			e := transports["a"].InstallSnapshot(raft.ServerID(to), raft.ServerAddress(peers[to]), &raft.InstallSnapshotRequest{Term: 2, LastLogIndex: 2, LastLogTerm: 1, Size: int64(len(payload))}, &resp, bytes.NewReader(payload))
			if e == nil && !resp.Success {
				e = errors.New("snapshot rejected")
			}
			result <- e
		}()
		select {
		case rpc := <-transports[to].Consumer():
			req, ok := rpc.Command.(*raft.InstallSnapshotRequest)
			if !ok || req.Size != int64(len(payload)) {
				rpc.Respond(nil, errors.New("bad snapshot"))
				t.Fatal("incorrect snapshot header")
			}
			got, e := io.ReadAll(rpc.Reader)
			if e != nil || !bytes.Equal(got, payload) {
				rpc.Respond(nil, errors.New("bad snapshot stream"))
				t.Fatalf("snapshot bytes differ: %v", e)
			}
			rpc.Respond(&raft.InstallSnapshotResponse{Term: 2, Success: true}, nil)
		case <-time.After(3 * time.Second):
			t.Fatal("snapshot RPC not received")
		}
		select {
		case e := <-result:
			if e != nil {
				t.Fatal(e)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("snapshot response hung")
		}
	}
}

func waitTLSStreamCounts(t *testing.T, s *TLSStream, active, handshakes int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		s.mu.Lock()
		a, h := len(s.active), len(s.handshakes)
		s.mu.Unlock()
		if a == active && h == handshakes {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("connections active/handshakes = %d/%d, want %d/%d", a, h, active, handshakes)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestTLSStreamCapacity(t *testing.T) {
	t.Run("handshakes", func(t *testing.T) {
		p := newTLSStreamPKI(t)
		ls, peers := tlsStreamListeners(t, "a", "b")
		s := startTLSStream(t, p, "a", "cluster", peers, ls["a"], 10*time.Second)
		var conns []net.Conn
		defer func() {
			for _, c := range conns {
				c.Close()
			}
		}()
		for i := 0; i < 32; i++ {
			c, e := net.DialTimeout("tcp", peers["a"], time.Second)
			if e != nil {
				t.Fatal(e)
			}
			conns = append(conns, c)
		}
		waitTLSStreamCounts(t, s, 0, 32)
		c, e := net.DialTimeout("tcp", peers["a"], time.Second)
		if e != nil {
			t.Fatal(e)
		}
		defer c.Close()
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		_, e = c.Read(make([]byte, 1))
		if e == nil {
			t.Fatal("33rd handshake admitted")
		}
		if ne, ok := e.(net.Error); ok && ne.Timeout() {
			t.Fatal("over-capacity connection not closed")
		}
		conns[0].Close()
		waitTLSStreamCounts(t, s, 0, 31)
	})
	t.Run("total", func(t *testing.T) {
		p := newTLSStreamPKI(t)
		ls, peers := tlsStreamListeners(t, "a", "b")
		a := startTLSStream(t, p, "a", "cluster", peers, ls["a"], 5*time.Second)
		b := startTLSStream(t, p, "b", "cluster", peers, ls["b"], 5*time.Second)
		var outgoing, incoming []net.Conn
		defer func() {
			for _, c := range append(outgoing, incoming...) {
				c.Close()
			}
		}()
		for i := 0; i < 128; i++ {
			c, e := a.Dial(raft.ServerAddress(peers["b"]), time.Second)
			if e != nil {
				t.Fatalf("Dial %d: %v", i, e)
			}
			outgoing = append(outgoing, c)
			c, e = b.Accept()
			if e != nil {
				t.Fatal(e)
			}
			incoming = append(incoming, c)
		}
		waitTLSStreamCounts(t, a, 128, 0)
		waitTLSStreamCounts(t, b, 128, 0)
		if c, e := a.Dial(raft.ServerAddress(peers["b"]), time.Second); e == nil {
			c.Close()
			t.Fatal("129th outgoing connection admitted")
		}
		_, config := p.configs(t, "a")
		config.ServerName = "127.0.0.1"
		c, e := tls.DialWithDialer(&net.Dialer{Timeout: time.Second}, "tcp", peers["b"], config)
		if e == nil {
			c.Close()
			t.Fatal("129th incoming connection admitted")
		}
		outgoing[0].Close()
		incoming[0].Close()
		waitTLSStreamCounts(t, a, 127, 0)
		waitTLSStreamCounts(t, b, 127, 0)
		replacement, e := a.Dial(raft.ServerAddress(peers["b"]), time.Second)
		if e != nil {
			t.Fatalf("capacity not released: %v", e)
		}
		defer replacement.Close()
		d, e := b.Accept()
		if e != nil {
			t.Fatal(e)
		}
		defer d.Close()
	})
}

func TestTLSStreamCloseDeliveredConnections(t *testing.T) {
	p := newTLSStreamPKI(t)
	ls, peers := tlsStreamListeners(t, "a", "b")
	a := startTLSStream(t, p, "a", "cluster", peers, ls["a"], time.Second)
	b := startTLSStream(t, p, "b", "cluster", peers, ls["b"], time.Second)
	for _, s := range []*TLSStream{a, b} {
		if c, e := s.Dial(raft.ServerAddress(s.Addr().String()), time.Second); c != nil || !errors.Is(e, ErrIdentity) {
			t.Fatalf("self Dial: %v, %v", c, e)
		}
	}
	out, e := a.Dial(raft.ServerAddress(peers["b"]), time.Second)
	if e != nil {
		t.Fatal(e)
	}
	defer out.Close()
	in, e := b.Accept()
	if e != nil {
		t.Fatal(e)
	}
	defer in.Close()
	reads := make(chan error, 2)
	for _, c := range []net.Conn{out, in} {
		go func(c net.Conn) { _, e := c.Read(make([]byte, 1)); reads <- e }(c)
	}
	closed := make(chan struct{})
	go func() { a.Close(); b.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close hung with delivered connections")
	}
	for i := 0; i < 2; i++ {
		select {
		case e := <-reads:
			if e == nil {
				t.Fatal("Read succeeded after Close")
			}
		case <-time.After(time.Second):
			t.Fatal("delivered Read not interrupted")
		}
	}
}

func TestTLSStreamCloseCancelsDial(t *testing.T) {
	p := newTLSStreamPKI(t)
	ls, peers := tlsStreamListeners(t, "a", "b")
	s := startTLSStream(t, p, "a", "cluster", peers, ls["a"], 10*time.Second)
	result := make(chan error, 1)
	go func() {
		c, e := s.Dial(raft.ServerAddress(peers["b"]), 10*time.Second)
		if c != nil {
			c.Close()
		}
		result <- e
	}()
	raw, e := ls["b"].Accept()
	if e != nil {
		t.Fatal(e)
	}
	defer raw.Close()
	waitTLSStreamCounts(t, s, 0, 1)
	closed := make(chan struct{})
	go func() { s.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close blocked on outgoing handshake")
	}
	select {
	case e := <-result:
		if e == nil {
			t.Fatal("canceled Dial succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel Dial")
	}
}

func TestTLSStreamSharedHandshakeBudget(t *testing.T) {
	p := newTLSStreamPKI(t)
	ls, peers := tlsStreamListeners(t, "a", "b")
	s := startTLSStream(t, p, "a", "cluster", peers, ls["a"], 300*time.Millisecond)
	server, _ := p.configs(t, "b")
	server.GetConfigForClient = func(*tls.ClientHelloInfo) (*tls.Config, error) { time.Sleep(180 * time.Millisecond); return nil, nil }
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		raw, e := ls["b"].Accept()
		if e != nil {
			return
		}
		defer raw.Close()
		c := tls.Server(raw, server)
		_ = c.SetDeadline(time.Now().Add(2 * time.Second))
		if c.Handshake() != nil {
			return
		}
		if _, e = readStreamHello(c); e != nil {
			return
		}
		time.Sleep(180 * time.Millisecond)
		h := s.hello
		h.ID = "b"
		_ = writeStreamHello(c, h)
	}()
	start := time.Now()
	c, e := s.Dial(raft.ServerAddress(peers["b"]), time.Second)
	if c != nil {
		c.Close()
	}
	if e == nil {
		t.Error("TLS and hello received separate timeout budgets")
	}
	if time.Since(start) > 650*time.Millisecond {
		t.Error("Dial exceeded shared budget")
	}
	select {
	case <-finished:
	case <-time.After(3 * time.Second):
		t.Fatal("test server did not finish")
	}
}

func TestTLSStreamInvalidConfigRetainsListenerOwnership(t *testing.T) {
	p := newTLSStreamPKI(t)
	for _, kind := range []string{"local-uri", "insecure-client", "no-client-auth", "negative-timeout", "duplicate-address", "missing-local", "dynamic-certificate"} {
		t.Run(kind, func(t *testing.T) {
			ls, peers := tlsStreamListeners(t, "a", "b")
			server, client := p.configs(t, "a")
			config := TLSStreamConfig{ID: "a", ClusterID: "cluster", Peers: peers, Listener: ls["a"], ServerTLS: server, ClientTLS: client}
			switch kind {
			case "local-uri":
				server, client = p.configs(t, "b")
				config.ServerTLS, config.ClientTLS = server, client
			case "insecure-client":
				client.InsecureSkipVerify = true
			case "no-client-auth":
				server.ClientAuth = tls.NoClientCert
			case "negative-timeout":
				config.Timeout = -time.Second
			case "duplicate-address":
				peers["b"] = peers["a"]
			case "missing-local":
				delete(peers, "a")
			case "dynamic-certificate":
				server.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return &server.Certificates[0], nil }
			}
			s, err := ListenTLS(config)
			if err == nil {
				s.Close()
				t.Fatal("invalid configuration accepted")
			}
			accepted := make(chan error, 1)
			go func() {
				c, e := ls["a"].Accept()
				if c != nil {
					c.Close()
				}
				accepted <- e
			}()
			c, e := net.DialTimeout("tcp", ls["a"].Addr().String(), time.Second)
			if e != nil {
				t.Fatalf("failed construction closed caller's listener: %v", e)
			}
			c.Close()
			select {
			case e := <-accepted:
				if e != nil {
					t.Fatal(e)
				}
			case <-time.After(time.Second):
				t.Fatal("caller listener unusable")
			}
		})
	}
}

func TestTLSStreamInvalidDial(t *testing.T) {
	p := newTLSStreamPKI(t)
	ls, peers := tlsStreamListeners(t, "a", "b")
	s := startTLSStream(t, p, "a", "cluster", peers, ls["a"], time.Second)
	for _, test := range []struct {
		address string
		timeout time.Duration
	}{{peers["a"], time.Second}, {"127.0.0.1:1", time.Second}, {peers["b"], 0}, {peers["b"], -time.Second}} {
		c, e := s.Dial(raft.ServerAddress(test.address), test.timeout)
		if c != nil {
			c.Close()
		}
		if !errors.Is(e, ErrIdentity) {
			t.Fatalf("Dial(%q,%s): %v", test.address, test.timeout, e)
		}
	}
}
