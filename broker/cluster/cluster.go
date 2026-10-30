// Package cluster is an experimental direct-connection prototype. Its handshake
// authenticates shared-secret possession over mutually authenticated TLS.
// It does not provide message routing, persistence, or replication. Seeds are
// static direct connections; a full mesh requires configuring enough seeds.
package cluster

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand"
	"net"
	"os"
	"regexp"
	"sort"
	"sync"
	"time"
)

// Config configures a node. OnMessage runs in frame order for each connection;
// callbacks on different connections may run concurrently. MaxCallbacks limits concurrent
// callbacks (default 32). A full per-connection queue disconnects the peer;
// queued messages are not guaranteed delivery. Close does
// not join callbacks; use WaitCallbacks from outside callbacks after closing.
// ServerTLS must require and verify client certificates using ClientCAs;
// ClientTLS must verify servers using RootCAs. Both need certificates. When
// ClientTLS.ServerName is empty, the seed host is used for name verification.
// Each leaf certificate must contain exactly one URI SAN, of the form
// urn:hmq:node:<NodeID>. Node IDs contain only ASCII letters, digits, '-' and
// '_'. The issuing CA must enforce exclusive ownership of each node ID.
type Config struct {
	NodeID            string
	ClusterID         string
	Secret            []byte
	ListenAddr        string
	Seeds             []string
	RetryInterval     time.Duration // Initial retry ceiling; subsequent ceilings double up to 30 seconds.
	HandshakeTimeout  time.Duration
	MaxFrameSize      uint32
	WriteTimeout      time.Duration
	OnMessage         func(peerID string, payload []byte)
	ServerTLS         *tls.Config
	ClientTLS         *tls.Config
	MaxCallbacks      int
	QueueSize         int
	HeartbeatInterval time.Duration
	HeartbeatTimeout  time.Duration
	// ReliableMembers enables fixed-membership authentication when nonempty.
	ReliableMembers []string
}

type hello struct{ Node, Cluster, Nonce, Version, Membership string }

var nodeIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

type proof struct{ MAC string }
type peer struct {
	conn      net.Conn
	outgoing  bool
	done      chan struct{}
	writeOnce sync.Once
	writeSlot chan struct{}
}
type Node struct {
	cfg            Config
	listener       net.Listener
	mu             sync.Mutex
	peers          map[string]*peer
	conns          map[net.Conn]struct{}
	closed         bool
	done           chan struct{}
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	callbackSlots  chan struct{}
	callbackWG     sync.WaitGroup
	broadcastSlots chan struct{}
	handshakeSlots chan struct{}
}

// ErrBroadcastBusy indicates that the node's in-flight broadcast limit was reached.
var ErrBroadcastBusy = errors.New("cluster: broadcast capacity exhausted")

// ErrPeerNotConnected means no authenticated connection exists for the NodeID.
var ErrPeerNotConnected = errors.New("cluster: peer not connected")

// ErrPeerWrite identifies a failed write to a connection selected for sending.
var ErrPeerWrite = errors.New("cluster: peer write failed")

const maxConcurrentBroadcasts = 256
const maxIncomingHandshakes = 128

// Bound all pending and established connections, including outbound attempts.
const maxConnections = 256

// Send sends one frame to the currently connected authenticated NodeID. It is
// best effort and does not acknowledge delivery or retry after reconnection.
// The caller must not mutate payload until Send returns. Send shares the
// broadcast concurrency limit and returns ErrBroadcastBusy when it is full.
// A write failure wraps both ErrPeerWrite and the underlying error.
func (n *Node) Send(id string, payload []byte) error {
	return n.SendContext(context.Background(), id, payload)
}

// SendContext bounds write-slot waiting and socket output by both ctx and the
// configured write timeout. Cancellation after output begins discards the peer
// connection: a potentially partial frame must never be followed by a new one.
// Cancellation while waiting for the slot does not interrupt its current owner.
func (n *Node) SendContext(ctx context.Context, id string, payload []byte) error {
	if ctx == nil {
		return errors.New("cluster: nil send context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return net.ErrClosed
	}
	select {
	case n.broadcastSlots <- struct{}{}:
	default:
		n.mu.Unlock()
		return ErrBroadcastBusy
	}
	p := n.peers[id]
	n.mu.Unlock()
	defer func() { <-n.broadcastSlots }()
	if p == nil {
		return ErrPeerNotConnected
	}
	if uint64(len(payload)) > uint64(n.cfg.MaxFrameSize) {
		return errors.New("cluster: frame too large")
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := n.writeFrameContext(ctx, p, header[:], payload); err != nil {
		return fmt.Errorf("%w: %w", ErrPeerWrite, err)
	}
	return nil
}

// Start listens immediately and reconnects to static seeds until Close.
func Start(c Config) (*Node, error) {
	if !nodeIDPattern.MatchString(c.NodeID) || c.ClusterID == "" || len(c.Secret) == 0 {
		return nil, errors.New("cluster: node ID, cluster ID and secret are required")
	}
	if c.ServerTLS == nil || c.ClientTLS == nil || c.ServerTLS.ClientAuth != tls.RequireAndVerifyClientCert || c.ServerTLS.ClientCAs == nil || c.ServerTLS.GetConfigForClient != nil || (len(c.ServerTLS.Certificates) == 0 && c.ServerTLS.GetCertificate == nil) || c.ClientTLS.InsecureSkipVerify || c.ClientTLS.RootCAs == nil || (len(c.ClientTLS.Certificates) == 0 && c.ClientTLS.GetClientCertificate == nil) {
		return nil, errors.New("cluster: explicit mutual TLS configuration required")
	}
	c.ServerTLS = c.ServerTLS.Clone()
	c.ClientTLS = c.ClientTLS.Clone()
	if c.MaxCallbacks <= 0 {
		c.MaxCallbacks = 32
	}
	if c.QueueSize <= 0 {
		c.QueueSize = 256
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 5 * time.Second
	}
	if c.HeartbeatTimeout <= 0 {
		c.HeartbeatTimeout = 3 * c.HeartbeatInterval
	}
	if c.HeartbeatTimeout <= c.HeartbeatInterval || c.MaxFrameSize >= ^uint32(0) {
		return nil, errors.New("cluster: invalid heartbeat or frame limit")
	}
	if c.ListenAddr == "" {
		c.ListenAddr = "127.0.0.1:0"
	}
	if c.RetryInterval <= 0 {
		c.RetryInterval = time.Second
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = 5 * time.Second
	}
	if c.MaxFrameSize == 0 {
		c.MaxFrameSize = 1024 * 1024
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 5 * time.Second
	}
	c.Secret = append([]byte(nil), c.Secret...)
	c.Seeds = append([]string(nil), c.Seeds...)
	c.ReliableMembers = append([]string(nil), c.ReliableMembers...)
	if len(c.ReliableMembers) != 0 {
		sort.Strings(c.ReliableMembers)
		local := false
		for i, id := range c.ReliableMembers {
			if !nodeIDPattern.MatchString(id) || (i > 0 && id == c.ReliableMembers[i-1]) {
				return nil, errors.New("cluster: invalid reliable membership")
			}
			local = local || id == c.NodeID
		}
		if !local || len(c.ReliableMembers) < 2 {
			return nil, errors.New("cluster: invalid reliable membership")
		}
	}
	l, err := net.Listen("tcp", c.ListenAddr)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{cfg: c, listener: l, peers: make(map[string]*peer), conns: make(map[net.Conn]struct{}), done: make(chan struct{}), ctx: ctx, cancel: cancel, callbackSlots: make(chan struct{}, c.MaxCallbacks), broadcastSlots: make(chan struct{}, maxConcurrentBroadcasts), handshakeSlots: make(chan struct{}, maxIncomingHandshakes)}
	n.wg.Add(1)
	go n.accept()
	seen := make(map[string]bool)
	for _, seed := range c.Seeds {
		if seen[seed] {
			continue
		}
		seen[seed] = true
		n.wg.Add(1)
		go n.connect(seed)
	}
	return n, nil
}

func (n *Node) Addr() string { return n.listener.Addr().String() }

// PeerStatus describes an authenticated connection without exposing configuration.
type PeerStatus struct {
	ID        string `json:"id"`
	Direction string `json:"direction"`
}

// Status is a point-in-time snapshot. Error history is not collected by Node.
type Status struct {
	State           string       `json:"state"`
	Peers           []PeerStatus `json:"peers"`
	ErrorsAvailable bool         `json:"errorsAvailable"`
}

func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	s := Status{State: "running", Peers: make([]PeerStatus, 0, len(n.peers))}
	if n.closed {
		s.State = "closed"
	}
	for id, p := range n.peers {
		direction := "incoming"
		if p.outgoing {
			direction = "outgoing"
		}
		s.Peers = append(s.Peers, PeerStatus{ID: id, Direction: direction})
	}
	sort.Slice(s.Peers, func(i, j int) bool { return s.Peers[i].ID < s.Peers[j].ID })
	return s
}

// Peers returns a sorted snapshot of authenticated node IDs.
func (n *Node) Peers() []string {
	n.mu.Lock()
	defer n.mu.Unlock()
	ids := make([]string, 0, len(n.peers))
	for id := range n.peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (n *Node) accept() {
	defer n.wg.Done()
	for {
		c, err := n.listener.Accept()
		if err != nil {
			return
		}
		select {
		case n.handshakeSlots <- struct{}{}:
			n.launch(tls.Server(c, n.cfg.ServerTLS), false)
		default:
			c.Close()
		}
	}
}
func (n *Node) connect(addr string) {
	defer n.wg.Done()
	backoff := n.cfg.RetryInterval
	maxBackoff := 30 * time.Second
	if backoff > maxBackoff {
		maxBackoff = backoff
	}
	for {
		select {
		case <-n.done:
			return
		default:
		}
		c, err := (&net.Dialer{Timeout: n.cfg.HandshakeTimeout}).DialContext(n.ctx, "tcp", addr)
		if err == nil {
			cfg := n.cfg.ClientTLS.Clone()
			if cfg.ServerName == "" {
				cfg.ServerName, _, _ = net.SplitHostPort(addr)
			}
			result := n.launch(tls.Client(c, cfg), true)
			var id string
			select {
			case id = <-result:
			case <-n.done:
				return
			}
			connectedAt := time.Now()
			for id != "" {
				n.mu.Lock()
				p := n.peers[id]
				n.mu.Unlock()
				if p == nil {
					break
				}
				select {
				case <-p.done:
				case <-n.done:
					return
				}
			}
			if id != "" && time.Since(connectedAt) >= n.cfg.HeartbeatTimeout {
				backoff = n.cfg.RetryInterval
			}
		}
		if !waitRetry(n.done, retryDelay(backoff, mathrand.Int63n)) {
			return
		}
		backoff = nextRetry(backoff, maxBackoff)
	}
}

// retryDelay chooses equal jitter in [ceiling/2, ceiling].
func retryDelay(ceiling time.Duration, random func(int64) int64) time.Duration {
	half := ceiling / 2
	return half + time.Duration(random(int64(ceiling-half)+1))
}

func nextRetry(current, max time.Duration) time.Duration {
	if current >= max-current {
		return max
	}
	return current * 2
}

func waitRetry(done <-chan struct{}, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-done:
		return false
	case <-timer.C:
		return true
	}
}
func (n *Node) launch(c net.Conn, outgoing bool) <-chan string {
	result := make(chan string, 1)
	n.mu.Lock()
	if n.closed || len(n.conns) >= maxConnections {
		n.mu.Unlock()
		c.Close()
		if !outgoing {
			<-n.handshakeSlots
		}
		result <- ""
		return result
	}
	n.conns[c] = struct{}{}
	n.wg.Add(1)
	n.mu.Unlock()
	go func() {
		defer n.wg.Done()
		defer close(result)
		defer func() { n.mu.Lock(); delete(n.conns, c); n.mu.Unlock(); c.Close() }()
		id, reader, err := n.handshake(c)
		if !outgoing {
			<-n.handshakeSlots
		}
		if err != nil {
			return
		}
		p := &peer{conn: c, outgoing: outgoing, done: make(chan struct{})}
		n.mu.Lock()
		if n.closed {
			n.mu.Unlock()
			return
		}
		old := n.peers[id]
		preferred := outgoing == (n.cfg.NodeID < id)
		if old != nil && (!preferred || old.outgoing == outgoing) {
			n.mu.Unlock()
			result <- id
			return
		}
		n.peers[id] = p
		n.mu.Unlock()
		result <- id
		if old != nil {
			old.conn.Close()
		}
		stop := make(chan struct{})
		heartbeatDone := make(chan struct{})
		go func() {
			defer close(heartbeatDone)
			ticker := time.NewTicker(n.cfg.HeartbeatInterval)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					var header [4]byte
					binary.BigEndian.PutUint32(header[:], ^uint32(0))
					if n.writeFrame(p, header[:], nil) != nil {
						c.Close()
						return
					}
				}
			}
		}()
		n.receivePeer(id, reader, c)
		c.Close()
		close(stop)
		<-heartbeatDone
		n.mu.Lock()
		if n.peers[id] == p {
			delete(n.peers, id)
		}
		n.mu.Unlock()
		close(p.done)
	}()
	return result
}
func (n *Node) handshake(c net.Conn) (string, io.Reader, error) {
	c.SetDeadline(time.Now().Add(n.cfg.HandshakeTimeout))
	defer c.SetDeadline(time.Time{})
	nonce := make([]byte, 32)
	if _, err := rand.Read(nonce); err != nil {
		return "", nil, err
	}
	membership := ""
	if len(n.cfg.ReliableMembers) != 0 {
		encoded, _ := json.Marshal(n.cfg.ReliableMembers)
		digest := sha256.Sum256(encoded)
		membership = hex.EncodeToString(digest[:])
	}
	me := hello{Node: n.cfg.NodeID, Cluster: n.cfg.ClusterID, Nonce: hex.EncodeToString(nonce), Version: "3", Membership: membership}
	enc := json.NewEncoder(c)
	dec := json.NewDecoder(io.LimitReader(c, 8192))
	if err := enc.Encode(me); err != nil {
		return "", nil, err
	}
	var other hello
	if err := dec.Decode(&other); err != nil {
		return "", nil, err
	}
	tc, ok := c.(*tls.Conn)
	if !ok {
		return "", nil, errors.New("cluster: TLS connection required")
	}
	state := tc.ConnectionState()
	if len(state.VerifiedChains) == 0 || len(state.PeerCertificates) == 0 || !nodeIDPattern.MatchString(other.Node) {
		return "", nil, errors.New("cluster: unverified peer identity")
	}
	uris := state.PeerCertificates[0].URIs
	if len(uris) != 1 || uris[0].String() != "urn:hmq:node:"+other.Node {
		return "", nil, errors.New("cluster: certificate node identity mismatch")
	}
	b, err := hex.DecodeString(other.Nonce)
	if err != nil || len(b) != 32 || other.Cluster != me.Cluster || other.Node == "" || other.Node == me.Node || other.Version != me.Version || other.Membership != me.Membership {
		return "", nil, errors.New("cluster: invalid peer hello")
	}
	if len(n.cfg.ReliableMembers) != 0 {
		i := sort.SearchStrings(n.cfg.ReliableMembers, other.Node)
		if i == len(n.cfg.ReliableMembers) || n.cfg.ReliableMembers[i] != other.Node {
			return "", nil, errors.New("cluster: peer outside reliable membership")
		}
	}
	mac := func(a, b hello) string {
		h := hmac.New(sha256.New, n.cfg.Secret)
		json.NewEncoder(h).Encode([]string{"hmq-cluster-v3", a.Cluster, a.Node, a.Nonce, a.Version, a.Membership, b.Node, b.Nonce, b.Version, b.Membership})
		return hex.EncodeToString(h.Sum(nil))
	}
	if err := enc.Encode(proof{mac(me, other)}); err != nil {
		return "", nil, err
	}
	var p proof
	if err := dec.Decode(&p); err != nil {
		return "", nil, err
	}
	got, err := hex.DecodeString(p.MAC)
	if err != nil {
		return "", nil, errors.New("cluster: invalid proof")
	}
	expected, _ := hex.DecodeString(mac(other, me))
	if !hmac.Equal(got, expected) {
		return "", nil, errors.New("cluster: authentication failed")
	}
	r := io.MultiReader(dec.Buffered(), c)
	var delimiter [1]byte
	if _, err := io.ReadFull(r, delimiter[:]); err != nil {
		return "", nil, err
	}
	if delimiter[0] != '\n' {
		return "", nil, errors.New("cluster: invalid proof delimiter")
	}
	return other.Node, r, nil
}

// Broadcast sends a four-byte big-endian length and payload to each currently
// connected peer. It is best effort, does not forward or acknowledge delivery,
// and reports failures by peer ID. Write failures wrap ErrPeerWrite and the
// underlying error. The empty key denotes a node-level error.
// The caller must not mutate payload until Broadcast returns.
// At most 256 broadcasts can be in flight per node; excess calls immediately
// return ErrBroadcastBusy under the empty key, without sending any frames.
func (n *Node) Broadcast(payload []byte) map[string]error {
	failures := make(map[string]error)
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		failures[""] = net.ErrClosed
		return failures
	}
	select {
	case n.broadcastSlots <- struct{}{}:
	default:
		n.mu.Unlock()
		failures[""] = ErrBroadcastBusy
		return failures
	}
	defer func() { <-n.broadcastSlots }()
	peers := make(map[string]*peer, len(n.peers))
	for id, p := range n.peers {
		peers[id] = p
	}
	n.mu.Unlock()
	if uint64(len(payload)) > uint64(n.cfg.MaxFrameSize) {
		for id := range peers {
			failures[id] = errors.New("cluster: frame too large")
		}
		if len(peers) == 0 {
			failures[""] = errors.New("cluster: frame too large")
		}
		return failures
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	var wg sync.WaitGroup
	var failuresMu sync.Mutex
	for id, p := range peers {
		wg.Add(1)
		go func(id string, p *peer) {
			defer wg.Done()
			if err := n.writeFrame(p, header[:], payload); err != nil {
				failuresMu.Lock()
				failures[id] = fmt.Errorf("%w: %w", ErrPeerWrite, err)
				failuresMu.Unlock()
				p.conn.Close()
			}
		}(id, p)
	}
	wg.Wait()
	return failures
}

func (n *Node) writeFrame(p *peer, header, payload []byte) error {
	_, err := n.writeFrameContext(context.Background(), p, header, payload)
	return err
}

// A socket deadline can fire before the context's timer goroutine is scheduled.
// Preserve the caller's deadline cause even in that ordering.
func sendContextError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

// started is conservative: once socket deadlines are changed, an error makes
// the connection unsafe to reuse even if no complete frame was observed.
func (n *Node) writeFrameContext(ctx context.Context, p *peer, header, payload []byte) (started bool, err error) {
	if ctx == nil {
		return false, errors.New("cluster: nil write context")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	p.writeOnce.Do(func() { p.writeSlot = make(chan struct{}, 1) })
	deadline := time.Now().Add(n.cfg.WriteTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case p.writeSlot <- struct{}{}:
	case <-timer.C:
		if err := sendContextError(ctx); err != nil {
			return false, err
		}
		return false, os.ErrDeadlineExceeded
	case <-ctx.Done():
		return false, ctx.Err()
	case <-n.done:
		return false, net.ErrClosed
	case <-p.done:
		return false, net.ErrClosed
	}
	defer func() {
		if started && err != nil {
			// Abort before handing the slot to another sender. TLS Close may
			// extend its own deadline to send close_notify, so close the socket.
			if secured, ok := p.conn.(*tls.Conn); ok {
				_ = secured.NetConn().Close()
			} else {
				_ = p.conn.Close()
			}
		}
		<-p.writeSlot
	}()
	select {
	case <-n.done:
		return false, net.ErrClosed
	case <-p.done:
		return false, net.ErrClosed
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}
	if !time.Now().Before(deadline) {
		if err := sendContextError(ctx); err != nil {
			return false, err
		}
		return false, os.ErrDeadlineExceeded
	}
	if err := p.conn.SetWriteDeadline(deadline); err != nil {
		return true, err
	}
	cancelled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = p.conn.SetWriteDeadline(time.Now()); close(cancelled) })
	_, err = io.Copy(p.conn, io.MultiReader(bytes.NewReader(header), bytes.NewReader(payload)))
	if !stop() {
		<-cancelled
	}
	if clearErr := p.conn.SetWriteDeadline(time.Time{}); err == nil {
		err = clearErr
	}
	if ctxErr := sendContextError(ctx); err != nil && ctxErr != nil {
		err = errors.Join(err, ctxErr)
	}
	return true, err
}

func (n *Node) receivePeer(id string, r io.Reader, conn net.Conn) {
	queue := make(chan []byte, n.cfg.QueueSize)
	stop := make(chan struct{})
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		for {
			select {
			case <-stop:
				return
			case payload := <-queue:
				select {
				case n.callbackSlots <- struct{}{}:
				case <-stop:
					return
				case <-n.done:
					return
				}
				finished := make(chan struct{})
				n.callbackWG.Add(1)
				go func() {
					defer n.callbackWG.Done()
					defer close(finished)
					defer func() { <-n.callbackSlots }()
					n.cfg.OnMessage(id, payload)
				}()
				select {
				case <-finished:
				case <-stop:
					return
				case <-n.done:
					return
				}
			}
		}
	}()
	defer func() { close(stop); <-workerDone }()
	for {
		conn.SetReadDeadline(time.Now().Add(n.cfg.HeartbeatTimeout))
		var header [4]byte
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return
		}
		size := binary.BigEndian.Uint32(header[:])
		if size == ^uint32(0) {
			continue
		}
		if size > n.cfg.MaxFrameSize || uint64(size) > uint64(^uint(0)>>1) {
			return
		}
		payload := make([]byte, int(size))
		if _, err := io.ReadFull(r, payload); err != nil {
			return
		}
		if n.cfg.OnMessage != nil {
			select {
			case queue <- payload:
			default:
				return
			}
		}
	}
}

func (n *Node) receive(id string, r io.Reader) {
	for {
		var header [4]byte
		if _, err := io.ReadFull(r, header[:]); err != nil {
			return
		}
		size := binary.BigEndian.Uint32(header[:])
		if size > n.cfg.MaxFrameSize || uint64(size) > uint64(^uint(0)>>1) {
			return
		}
		payload := make([]byte, int(size))
		if _, err := io.ReadFull(r, payload); err != nil {
			return
		}
		if n.cfg.OnMessage != nil {
			select {
			case n.callbackSlots <- struct{}{}:
			case <-n.done:
				return
			}
			finished := make(chan struct{})
			n.callbackWG.Add(1)
			go func() {
				defer n.callbackWG.Done()
				defer close(finished)
				defer func() { <-n.callbackSlots }()
				n.cfg.OnMessage(id, payload)
			}()
			select {
			case <-finished:
			case <-n.done:
				return
			}
		}
	}
}

// WaitCallbacks waits for callbacks after Close has returned. Do not call it
// from a callback. Close must be called first to prevent new callbacks.
func (n *Node) WaitCallbacks() { n.Close(); n.callbackWG.Wait() }

// Close terminates listeners, pending handshakes and established connections.
// Already dispatched callbacks may continue after Close returns.
func (n *Node) Close() error {
	n.mu.Lock()
	if !n.closed {
		n.closed = true
		n.cancel()
		close(n.done)
		n.listener.Close()
		for c := range n.conns {
			c.Close()
		}
	}
	n.mu.Unlock()
	n.wg.Wait()
	return nil
}
