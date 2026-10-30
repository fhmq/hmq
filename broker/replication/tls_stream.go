package replication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

type TLSStreamConfig struct {
	ID, ClusterID, Listen string
	Peers                 map[string]string
	ServerTLS, ClientTLS  *tls.Config
	Timeout               time.Duration
	// Listener, if supplied, transfers ownership on successful construction.
	Listener net.Listener
}
type streamAddress string

func (a streamAddress) Network() string { return "tcp" }
func (a streamAddress) String() string  { return string(a) }

type TLSStream struct {
	listener       net.Listener
	address        net.Addr
	server, client *tls.Config
	ids            map[string]string // advertised address -> member ID
	hello          streamHello
	timeout        time.Duration
	done           chan struct{}
	ready          chan net.Conn
	mu             sync.Mutex
	handshakes     map[net.Conn]struct{}
	active         map[net.Conn]struct{}
	ctx            context.Context
	cancel         context.CancelFunc
	closed         bool
	once           sync.Once
	wg             sync.WaitGroup
}
type streamHello struct {
	Version                 int
	ID, Cluster, Membership string
	Protocol                string
}

var streamID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
var _ raft.StreamLayer = (*TLSStream)(nil)

func streamCertificateID(cert *x509.Certificate) string {
	if cert == nil || len(cert.URIs) != 1 {
		return ""
	}
	u := cert.URIs[0]
	const prefix = "hmq:node:"
	if u.Scheme != "urn" || len(u.Opaque) <= len(prefix) || u.Opaque[:len(prefix)] != prefix {
		return ""
	}
	id := u.Opaque[len(prefix):]
	if !streamID.MatchString(id) || u.String() != "urn:hmq:node:"+id {
		return ""
	}
	return id
}
func localStreamCertificate(config *tls.Config, id string) bool {
	if len(config.Certificates) != 1 || len(config.Certificates[0].Certificate) == 0 {
		return false
	}
	cert, err := x509.ParseCertificate(config.Certificates[0].Certificate[0])
	return err == nil && streamCertificateID(cert) == id
}

// ListenTLS authenticates every connection against a fixed member map before
// handing it to Raft. It uses a separate streaming connection, not MQTT frames.
func ListenTLS(c TLSStreamConfig) (*TLSStream, error) {
	if !streamID.MatchString(c.ID) || !streamID.MatchString(c.ClusterID) || len(c.Peers) < 1 || c.ServerTLS == nil || c.ClientTLS == nil {
		return nil, ErrInvalid
	}
	server, client := c.ServerTLS.Clone(), c.ClientTLS.Clone()
	if server.ClientAuth != tls.RequireAndVerifyClientCert || server.ClientCAs == nil || client.RootCAs == nil || client.InsecureSkipVerify || server.InsecureSkipVerify || !localStreamCertificate(server, c.ID) || !localStreamCertificate(client, c.ID) {
		return nil, ErrIdentity
	}
	// Dynamic certificate/config callbacks could bypass the fixed local identity.
	if server.GetConfigForClient != nil || server.GetCertificate != nil || client.GetClientCertificate != nil {
		return nil, ErrIdentity
	}
	if server.MinVersion < tls.VersionTLS12 {
		server.MinVersion = tls.VersionTLS12
	}
	if client.MinVersion < tls.VersionTLS12 {
		client.MinVersion = tls.VersionTLS12
	}
	if c.Timeout == 0 {
		c.Timeout = 5 * time.Second
	}
	if c.Timeout < 0 {
		return nil, ErrInvalid
	}
	ids := make(map[string]string, len(c.Peers))
	var members []string
	for id, address := range c.Peers {
		host, port, err := net.SplitHostPort(address)
		n, e := strconv.Atoi(port)
		if !streamID.MatchString(id) || err != nil || e != nil || host == "" || n < 1 || n > 65535 || ids[address] != "" {
			return nil, ErrInvalid
		}
		ids[address] = id
		members = append(members, id+"\x00"+address)
	}
	address, ok := c.Peers[c.ID]
	if !ok {
		return nil, ErrInvalid
	}
	sort.Strings(members)
	encoded, _ := json.Marshal(members)
	digest := sha256.Sum256(encoded)
	l := c.Listener
	if l == nil {
		var err error
		l, err = net.Listen("tcp", c.Listen)
		if err != nil {
			return nil, err
		}
	}
	s := &TLSStream{listener: l, address: streamAddress(address), server: server, client: client, ids: ids, timeout: c.Timeout, hello: streamHello{Version: 1, ID: c.ID, Cluster: c.ClusterID, Membership: hex.EncodeToString(digest[:]), Protocol: "hmq-raft/1"}, done: make(chan struct{}), ready: make(chan net.Conn), handshakes: make(map[net.Conn]struct{})}
	s.active = make(map[net.Conn]struct{})
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

func (s *TLSStream) Addr() net.Addr { return s.address }
func (s *TLSStream) Accept() (net.Conn, error) {
	select {
	case <-s.done:
		return nil, net.ErrClosed
	case conn := <-s.ready:
		s.mu.Lock()
		delete(s.handshakes, conn)
		closed := s.closed
		if !closed {
			s.active[conn] = struct{}{}
		}
		s.mu.Unlock()
		if closed {
			conn.Close()
			return nil, net.ErrClosed
		}
		return &streamConn{Conn: conn, owner: s}, nil
	}
}
func (s *TLSStream) acceptLoop() {
	defer s.wg.Done()
	for {
		raw, err := s.listener.Accept()
		if err != nil {
			s.stop()
			return
		}
		s.mu.Lock()
		if s.closed || len(s.handshakes) >= 32 || len(s.active)+len(s.handshakes) >= 128 {
			s.mu.Unlock()
			raw.Close()
			continue
		}
		conn := tls.Server(raw, s.server)
		s.handshakes[conn] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		deadline := time.Now().Add(s.timeout)
		go func() {
			defer s.wg.Done()
			if err := s.authenticate(conn, "", false, time.Until(deadline)); err == nil {
				select {
				case s.ready <- conn:
					return
				case <-s.done:
				}
			}
			conn.Close()
			s.mu.Lock()
			delete(s.handshakes, conn)
			s.mu.Unlock()
		}()
	}
}

func (s *TLSStream) stop() {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.cancel()
		close(s.done)
		var connections []net.Conn
		for conn := range s.handshakes {
			connections = append(connections, conn)
		}
		for conn := range s.active {
			connections = append(connections, conn)
		}
		s.mu.Unlock()
		_ = s.listener.Close()
		for _, conn := range connections {
			// tls.Conn.Close can reset its write deadline for close_notify.
			// Shutdown must abort the socket, not wait per peer for that alert.
			if secured, ok := conn.(*tls.Conn); ok {
				_ = secured.NetConn().Close()
			} else {
				_ = conn.Close()
			}
		}
	})
}
func (s *TLSStream) Close() error { s.stop(); s.wg.Wait(); return nil }

func (s *TLSStream) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	id, ok := s.ids[string(address)]
	if !ok || id == s.hello.ID || timeout <= 0 {
		return nil, ErrIdentity
	}
	select {
	case <-s.done:
		return nil, net.ErrClosed
	default:
	}
	if timeout > s.timeout {
		timeout = s.timeout
	}
	ctx, cancel := context.WithTimeout(s.ctx, timeout)
	defer cancel()
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", string(address))
	if err != nil {
		return nil, err
	}
	config := s.client.Clone()
	host, _, _ := net.SplitHostPort(string(address))
	config.ServerName = host
	conn := tls.Client(raw, config)
	s.mu.Lock()
	if s.closed || len(s.handshakes) >= 32 || len(s.active)+len(s.handshakes) >= 128 {
		s.mu.Unlock()
		conn.Close()
		return nil, net.ErrClosed
	}
	s.handshakes[conn] = struct{}{}
	s.wg.Add(1)
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.handshakes, conn); s.mu.Unlock(); s.wg.Done() }()
	deadline, _ := ctx.Deadline()
	if err := s.authenticate(conn, id, true, time.Until(deadline)); err != nil {
		conn.Close()
		return nil, err
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		conn.Close()
		return nil, net.ErrClosed
	}
	delete(s.handshakes, conn)
	s.active[conn] = struct{}{}
	s.mu.Unlock()
	return &streamConn{Conn: conn, owner: s}, nil
}

func (s *TLSStream) authenticate(conn *tls.Conn, expected string, outgoing bool, timeout time.Duration) error {
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	if err := conn.Handshake(); err != nil {
		return err
	}
	state := conn.ConnectionState()
	if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 {
		return ErrIdentity
	}
	id := streamCertificateID(state.PeerCertificates[0])
	member := false
	for _, peer := range s.ids {
		if peer == id {
			member = true
			break
		}
	}
	if !member || id == s.hello.ID || (expected != "" && id != expected) {
		return ErrIdentity
	}
	if outgoing {
		if err := writeStreamHello(conn, s.hello); err != nil {
			return err
		}
	}
	hello, err := readStreamHello(conn)
	if err != nil {
		return err
	}
	if hello.Protocol != "hmq-raft/1" || hello.Version != 1 || hello.ID != id || hello.Cluster != s.hello.Cluster || hello.Membership != s.hello.Membership {
		return ErrIdentity
	}
	if !outgoing {
		if err := writeStreamHello(conn, s.hello); err != nil {
			return err
		}
	}
	return conn.SetDeadline(time.Time{})
}

type streamConn struct {
	net.Conn
	owner *TLSStream
	once  sync.Once
	err   error
}

func (c *streamConn) Close() error {
	c.once.Do(func() { c.err = c.Conn.Close(); c.owner.mu.Lock(); delete(c.owner.active, c.Conn); c.owner.mu.Unlock() })
	return c.err
}
func writeStreamHello(w io.Writer, h streamHello) error {
	data, err := json.Marshal(h)
	if err != nil {
		return err
	}
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(len(data)))
	for _, part := range [][]byte{size[:], data} {
		n, err := w.Write(part)
		if err != nil {
			return err
		}
		if n != len(part) {
			return io.ErrShortWrite
		}
	}
	return nil
}
func readStreamHello(r io.Reader) (streamHello, error) {
	var h streamHello
	var size [4]byte
	if _, err := io.ReadFull(r, size[:]); err != nil {
		return h, err
	}
	n := binary.BigEndian.Uint32(size[:])
	if n == 0 || n > 1024 {
		return h, ErrIdentity
	}
	data := make([]byte, n)
	if _, err := io.ReadFull(r, data); err != nil {
		return h, err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&h); err != nil {
		return h, err
	}
	if !errors.Is(d.Decode(new(any)), io.EOF) {
		return h, ErrIdentity
	}
	return h, nil
}
