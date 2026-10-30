package cluster

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/url"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestReceiveProcessesFramesInOrder(t *testing.T) {
	started := make(chan string, 2)
	release := make(chan struct{})
	n := &Node{cfg: Config{MaxFrameSize: 10, OnMessage: func(_ string, p []byte) {
		started <- string(p)
		if string(p) == "a" {
			<-release
		}
	}}, done: make(chan struct{}), callbackSlots: make(chan struct{}, 2)}
	done := make(chan struct{})
	go func() { n.receive("peer", bytes.NewReader([]byte{0, 0, 0, 1, 'a', 0, 0, 0, 1, 'b'})); close(done) }()
	if got := <-started; got != "a" {
		t.Fatalf("first frame: %q", got)
	}
	select {
	case got := <-started:
		t.Fatalf("callback ran before first completed: %q", got)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case got := <-started:
		if got != "b" {
			t.Fatalf("second frame: %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("second callback did not run")
	}
	<-done
	n.callbackWG.Wait()
}

func TestReceiveRejectsMalformedFrames(t *testing.T) {
	for _, frame := range [][]byte{{0, 0}, {0, 0, 0, 5}, {0, 0, 0, 3, 'x'}} {
		calls := 0
		n := &Node{cfg: Config{MaxFrameSize: 4, OnMessage: func(string, []byte) { calls++ }}, done: make(chan struct{}), callbackSlots: make(chan struct{}, 1)}
		n.receive("peer", bytes.NewReader(frame))
		n.callbackWG.Wait()
		if calls != 0 {
			t.Fatalf("callback on malformed frame %v", frame)
		}
	}
}

func TestConcurrentCloseAndBroadcast(t *testing.T) {
	a := start(t, "a", "g", "s")
	start(t, "b", "g", "s", a.Addr())
	await(t, a, "b")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); a.Broadcast([]byte("frame")); a.Close() }()
	}
	wg.Wait()
	a.WaitCallbacks()
}

func testTLS(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	pair := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	return &tls.Config{Certificates: []tls.Certificate{pair}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}, &tls.Config{Certificates: []tls.Certificate{pair}, RootCAs: pool}
}

var tlsOnce sync.Once
var serverTLS, clientTLS *tls.Config

func configs(t *testing.T, id string) (*tls.Config, *tls.Config) {
	tlsOnce.Do(func() { serverTLS, clientTLS = testTLS(t) })
	ca, err := x509.ParseCertificate(serverTLS.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse("urn:hmq:node:" + id)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(2), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{u}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, serverTLS.Certificates[0].PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	pair := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
	s, c := serverTLS.Clone(), clientTLS.Clone()
	s.Certificates, c.Certificates = []tls.Certificate{pair}, []tls.Certificate{pair}
	return s, c
}

func TestSeedReconnectOnlyAfterDisconnect(t *testing.T) {
	a := start(t, "a", "group", "secret")
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var connections []net.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			upstream, err := net.Dial("tcp", a.Addr())
			if err != nil {
				c.Close()
				continue
			}
			mu.Lock()
			connections = append(connections, c)
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				defer upstream.Close()
				go func() { io.Copy(upstream, c); upstream.Close() }()
				io.Copy(c, upstream)
			}()
		}
	}()
	t.Cleanup(func() {
		l.Close()
		mu.Lock()
		for _, c := range connections {
			c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	b := start(t, "b", "group", "secret", l.Addr().String(), l.Addr().String())
	await(t, b, "a")
	time.Sleep(180 * time.Millisecond)
	mu.Lock()
	count := len(connections)
	if count > 0 {
		connections[0].Close()
	}
	mu.Unlock()
	if count != 1 {
		t.Fatalf("stable connection caused %d dials", count)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		count = len(connections)
		mu.Unlock()
		if count >= 2 && reflect.DeepEqual(b.Peers(), []string{"a"}) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if count != 2 {
		t.Fatalf("expected one reconnect, got %d dials", count)
	}
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	count = len(connections)
	mu.Unlock()
	if count != 2 {
		t.Fatalf("unexpected additional dials: %d", count)
	}
}

func TestConcurrentBroadcast(t *testing.T) {
	received := make(chan string, 400)
	makeNode := func(id string, seeds ...string) *Node {
		s, c := configs(t, id)
		n, err := Start(Config{ServerTLS: s, ClientTLS: c, NodeID: id, ClusterID: "g", Secret: []byte("s"), Seeds: seeds, RetryInterval: 20 * time.Millisecond, OnMessage: func(id string, data []byte) { received <- id + ":" + string(data) }})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { n.Close() })
		return n
	}
	a := makeNode("a")
	b := makeNode("b", a.Addr())
	c := makeNode("c", a.Addr(), b.Addr())
	await(t, a, "b", "c")
	await(t, b, "a", "c")
	await(t, c, "a", "b")
	var wg sync.WaitGroup
	for _, n := range []*Node{a, b, c} {
		for i := 0; i < 30; i++ {
			wg.Add(1)
			go func(n *Node, i int) {
				defer wg.Done()
				if errs := n.Broadcast([]byte(fmt.Sprint(i))); len(errs) != 0 {
					t.Errorf("broadcast: %v", errs)
				}
			}(n, i)
		}
	}
	wg.Wait()
	for i := 0; i < 180; i++ {
		select {
		case <-received:
		case <-time.After(3 * time.Second):
			t.Fatalf("received %d frames", i)
		}
	}
	a.Close()
	if len(a.Broadcast(nil)) == 0 {
		t.Fatal("closed broadcast succeeded")
	}
}

func TestCallbackBroadcastAndLimit(t *testing.T) {
	done := make(chan struct{}, 1)
	var a *Node
	var err error
	s, c := configs(t, "a")
	a, err = Start(Config{ServerTLS: s, ClientTLS: c, NodeID: "a", ClusterID: "g", Secret: []byte("s"), MaxFrameSize: 4, OnMessage: func(_ string, _ []byte) { a.Broadcast([]byte("ok")); done <- struct{}{} }})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b := start(t, "b", "g", "s", a.Addr())
	await(t, b, "a")
	if len(b.Broadcast([]byte("hi"))) != 0 {
		t.Fatal("broadcast failed")
	}
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("callback blocked")
	}
	if len(a.Broadcast([]byte("12345"))) == 0 {
		t.Fatal("oversize accepted")
	}
}

func start(t *testing.T, id, group, key string, seeds ...string) *Node {
	t.Helper()
	s, c := configs(t, id)
	n, err := Start(Config{ServerTLS: s, ClientTLS: c, NodeID: id, ClusterID: group, Secret: []byte(key), Seeds: seeds, RetryInterval: 20 * time.Millisecond, HandshakeTimeout: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close() })
	return n
}
func await(t *testing.T, n *Node, want ...string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if reflect.DeepEqual(n.Peers(), want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("peers: got %v want %v", n.Peers(), want)
}
func TestDirectNodesAndClose(t *testing.T) {
	a := start(t, "a", "group", "secret")
	b := start(t, "b", "group", "secret", a.Addr())
	c := start(t, "c", "group", "secret", a.Addr(), b.Addr())
	await(t, a, "b", "c")
	await(t, b, "a", "c")
	await(t, c, "a", "b")
	b.Close()
	await(t, a, "c")
	await(t, c, "a")
}

func TestReliableMembershipHandshake(t *testing.T) {
	makeNode := func(id string, members []string, seeds ...string) *Node {
		s, c := configs(t, id)
		n, err := Start(Config{NodeID: id, ClusterID: "g", Secret: []byte("s"), ServerTLS: s, ClientTLS: c, ReliableMembers: members, Seeds: seeds, RetryInterval: 20 * time.Millisecond, HandshakeTimeout: 200 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { n.Close() })
		return n
	}
	a := makeNode("a", []string{"c", "b", "a"})
	b := makeNode("b", []string{"a", "b", "c"}, a.Addr())
	await(t, a, "b")
	await(t, b, "a")
	c := makeNode("c", nil, a.Addr())
	time.Sleep(500 * time.Millisecond)
	if len(c.Peers()) != 0 || !reflect.DeepEqual(a.Peers(), []string{"b"}) {
		t.Fatalf("mixed modes connected: a=%v c=%v", a.Peers(), c.Peers())
	}
}
func TestAuthenticationAndIdentity(t *testing.T) {
	a := start(t, "a", "group", "secret")
	badKey := start(t, "b", "group", "wrong", a.Addr())
	badGroup := start(t, "c", "other", "secret", a.Addr())
	sameID := start(t, "a", "group", "secret", a.Addr())
	time.Sleep(350 * time.Millisecond)
	for _, n := range []*Node{a, badKey, badGroup, sameID} {
		if p := n.Peers(); len(p) != 0 {
			t.Fatalf("unexpected peers: %v", p)
		}
	}
}
func TestInvalidConfig(t *testing.T) {
	if _, err := Start(Config{}); err == nil {
		t.Fatal("expected error")
	}
}

func TestTLSRejectsUntrustedAndPlaintext(t *testing.T) {
	a := start(t, "a", "group", "secret")
	s, c := testTLS(t)
	b, err := Start(Config{NodeID: "b", ClusterID: "group", Secret: []byte("secret"), ServerTLS: s, ClientTLS: c, Seeds: []string{a.Addr()}, RetryInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	plain, err := net.Dial("tcp", a.Addr())
	if err != nil {
		t.Fatal(err)
	}
	plain.SetDeadline(time.Now().Add(time.Second))
	plain.Write([]byte("not tls"))
	var buf [1]byte
	if _, err := plain.Read(buf[:]); err == nil {
		t.Fatal("plaintext accepted")
	}
	plain.Close()
	time.Sleep(250 * time.Millisecond)
	if len(a.Peers()) != 0 || len(b.Peers()) != 0 {
		t.Fatal("untrusted peer accepted")
	}
	if _, err := Start(Config{NodeID: "x", ClusterID: "g", Secret: []byte("s")}); err == nil {
		t.Fatal("missing TLS accepted")
	}
}

func TestCertificateCannotImpersonateNode(t *testing.T) {
	for _, outgoing := range []bool{false, true} {
		t.Run(fmt.Sprint(outgoing), func(t *testing.T) {
			s, c := configs(t, "attacker")
			a := start(t, "a", "group", "secret")
			cfg := Config{NodeID: "victim", ClusterID: "group", Secret: []byte("secret"), ServerTLS: s, ClientTLS: c, RetryInterval: 20 * time.Millisecond}
			if outgoing {
				cfg.Seeds = []string{a.Addr()}
			}
			b, err := Start(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			if !outgoing {
				attacker := start(t, "a2", "group", "secret", b.Addr())
				time.Sleep(250 * time.Millisecond)
				if len(attacker.Peers()) != 0 {
					t.Fatal("accepted impersonating server")
				}
			} else {
				time.Sleep(250 * time.Millisecond)
				if len(a.Peers()) != 0 || len(b.Peers()) != 0 {
					t.Fatal("accepted impersonating client")
				}
			}
		})
	}
}

func TestTLSRejectsEachUntrustedDirection(t *testing.T) {
	trustedServer, trustedClient := configs(t, "b")
	otherServer, otherClient := testTLS(t)
	for _, tc := range []struct {
		name           string
		server, client *tls.Config
	}{
		{"client certificate", otherServer, func() *tls.Config { c := otherClient.Clone(); c.RootCAs = trustedClient.RootCAs; return c }()},
		{"server certificate", trustedServer, func() *tls.Config { c := trustedClient.Clone(); c.RootCAs = otherClient.RootCAs; return c }()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := start(t, "a", "g", "s")
			b, err := Start(Config{NodeID: "b", ClusterID: "g", Secret: []byte("s"), ServerTLS: tc.server, ClientTLS: tc.client, Seeds: []string{a.Addr()}, RetryInterval: 20 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			time.Sleep(200 * time.Millisecond)
			if len(a.Peers()) != 0 || len(b.Peers()) != 0 {
				t.Fatal("untrusted connection accepted")
			}
		})
	}
}

func TestCallbackCloseAndWait(t *testing.T) {
	s, c := configs(t, "a")
	started := make(chan struct{})
	release := make(chan struct{})
	closed := make(chan struct{})
	var a *Node
	a, _ = Start(Config{NodeID: "a", ClusterID: "g", Secret: []byte("s"), ServerTLS: s, ClientTLS: c, MaxCallbacks: 1, OnMessage: func(string, []byte) { close(started); a.Close(); close(closed); <-release }})
	b := start(t, "b", "g", "s", a.Addr())
	await(t, b, "a")
	b.Broadcast([]byte("x"))
	<-started
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("close blocked in callback")
	}
	waited := make(chan struct{})
	go func() { a.WaitCallbacks(); close(waited) }()
	select {
	case <-waited:
		t.Fatal("wait returned early")
	case <-time.After(30 * time.Millisecond):
	}
	close(release)
	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("wait blocked")
	}
}
