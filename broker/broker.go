package broker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	encJson "encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/fhmq/hmq/v3/broker/cluster"
	"github.com/fhmq/hmq/v3/broker/lib/sessions"
	"github.com/fhmq/hmq/v3/broker/lib/topics"
	"github.com/fhmq/hmq/v3/broker/reliable/inbox"
	"github.com/fhmq/hmq/v3/broker/sessionstore"
	"github.com/fhmq/hmq/v3/plugins/auth"
	"github.com/fhmq/hmq/v3/plugins/bridge"
	"github.com/fhmq/hmq/v3/pool"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/net/websocket"
)

const (
	MessagePoolNum        = 1024
	MessagePoolMessageNum = 1024
)

type Message struct {
	client *client
	packet packets.ControlPacket
}

type Broker struct {
	mu               sync.Mutex
	connectMu        sync.Mutex
	connectLocks     map[string]*connectLock
	config           *Config
	tlsConfig        *tls.Config
	wpool            *pool.WorkerPool
	clients          sync.Map
	topicsMgr        *topics.Manager
	sessionMgr       *sessions.Manager
	sessionLog       *sessionstore.Store
	sessionIndex     *durableIndex
	sessionWindow    int                  // Zero selects the production default; tests may reduce it.
	durableRouteHook func(string, string) // Test barrier, configured before publishing.
	auth             auth.Auth
	bridgeMQ         bridge.BridgeMQ
	lifecycleMu      sync.Mutex
	stopped          chan struct{}
	closeOnce        sync.Once
	listeners        map[net.Listener]struct{}
	connections      map[net.Conn]struct{}
	servers          map[*http.Server]struct{}
	jobs             sync.WaitGroup
	ctx              context.Context
	cancel           context.CancelFunc
	clusterNode      *cluster.Node
	reliableRuntime  *reliableRuntime
	quorumRuntime    *quorumRuntime
	forwardMu        sync.RWMutex
	forwardPublish   func(*packets.PublishPacket) error
}

type connectLock struct {
	mu   sync.Mutex
	refs int
}

func (b *Broker) lockClientConnect(id string) func() {
	b.connectMu.Lock()
	if b.connectLocks == nil {
		b.connectLocks = make(map[string]*connectLock)
	}
	l := b.connectLocks[id]
	if l == nil {
		l = &connectLock{}
		b.connectLocks[id] = l
	}
	l.refs++
	b.connectMu.Unlock()
	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		b.connectMu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(b.connectLocks, id)
		}
		b.connectMu.Unlock()
	}
}

// withClientRoute serializes a routing decision with connection takeover.
// The callback must not acquire another client route lock or close a client.
func (b *Broker) withClientRoute(id string, route func(*client) error) error {
	unlock := b.lockClientConnect(id)
	defer unlock()
	value, ok := b.clients.Load(id)
	if !ok {
		return net.ErrClosed
	}
	c, ok := value.(*client)
	if !ok || c.isDisconnected() {
		return net.ErrClosed
	}
	return route(c)
}

// SetPublishForwarder replaces the default best-effort cluster forwarding.
// Set it to nil to restore the default. The callback may be called concurrently.
func (b *Broker) SetPublishForwarder(forward func(*packets.PublishPacket) error) {
	b.forwardMu.Lock()
	b.forwardPublish = forward
	b.forwardMu.Unlock()
}

func (b *Broker) forwardMessage(packet *packets.PublishPacket) error {
	if b.quorumEnabled() {
		return ErrQuorumRoutingUnavailable
	}
	b.forwardMu.RLock()
	forward := b.forwardPublish
	b.forwardMu.RUnlock()
	if forward != nil {
		return forward(packet)
	}
	if b.reliableRuntime != nil {
		data, err := encJson.Marshal(clusterPublish{packet.TopicName, packet.Payload, packet.Qos, packet.Retain})
		if err != nil {
			return err
		}
		return b.reliableRuntime.enqueue(data)
	}
	b.broadcastClusterMessage(packet)
	return nil
}

func newMessagePool() []chan *Message {
	pool := make([]chan *Message, 0)
	for i := 0; i < MessagePoolNum; i++ {
		ch := make(chan *Message, MessagePoolMessageNum)
		pool = append(pool, ch)
	}
	return pool
}

func getAdditionalLogFields(clientIdentifier string, conn net.Conn, additionalFields ...zapcore.Field) []zapcore.Field {
	var wsConn *websocket.Conn = nil
	var wsEnabled bool
	result := []zapcore.Field{}

	switch conn.(type) {
	case *websocket.Conn:
		wsEnabled = true
		wsConn = conn.(*websocket.Conn)
	case *net.TCPConn:
		wsEnabled = false
	}

	// add optional fields
	if len(additionalFields) > 0 {
		result = append(result, additionalFields...)
	}

	// add client ID
	result = append(result, zap.String("clientID", clientIdentifier))

	// add remote connection address
	if !wsEnabled && conn != nil {
		if conn.RemoteAddr() != nil {
			result = append(result, zap.String("addr", conn.RemoteAddr().String()))
		}
	} else if wsEnabled && wsConn != nil {
		if wsConn.Request() != nil {
			result = append(result, zap.String("addr", wsConn.Request().RemoteAddr))
		} else {
			result = append(result, zap.String("addr", wsConn.RemoteAddr().String()))
		}
	}

	return result
}

func NewBroker(config *Config) (*Broker, error) {
	if config == nil {
		config = DefaultConfig
	}
	configCopy := *config
	configCopy.Cluster.Seeds = append([]string(nil), config.Cluster.Seeds...)
	configCopy.Cluster.Reliable.MemberIDs = append([]string(nil), config.Cluster.Reliable.MemberIDs...)
	configCopy.Cluster.Quorum.Peers = make(map[string]string, len(config.Cluster.Quorum.Peers))
	for id, address := range config.Cluster.Quorum.Peers {
		configCopy.Cluster.Quorum.Peers[id] = address
	}
	config = &configCopy
	if err := validateSessionLogPath(config); err != nil {
		return nil, err
	}
	if err := validateReliableConfig(config.Cluster, config.RetainedLogPath); err != nil {
		return nil, err
	}
	if config.Router != "" || config.Cluster.Host != "" || config.Cluster.Port != "" {
		return nil, errors.New("legacy router configuration is unsupported")
	}
	if config.RetainedLogPath != "" && strings.TrimSpace(config.RetainedLogPath) == "" {
		return nil, errors.New("retained log path must not be blank")
	}
	c := config.Cluster
	if c.Listen == "" && (c.NodeID != "" || c.ClusterID != "" || c.Secret != "" || len(c.Seeds) != 0 || c.TLS != (ClusterTLSInfo{})) {
		return nil, errors.New("cluster listen address is required")
	}

	b := &Broker{
		config:      config,
		stopped:     make(chan struct{}),
		listeners:   make(map[net.Listener]struct{}),
		connections: make(map[net.Conn]struct{}),
		servers:     make(map[*http.Server]struct{}),
	}

	var err error
	if config.RetainedLogPath != "" {
		b.topicsMgr, err = topics.NewPersistentManager(config.RetainedLogPath)
	} else {
		b.topicsMgr, err = topics.NewManager("mem")
	}
	if err != nil {
		log.Error("new topic manager error", zap.Error(err))
		return nil, err
	}

	b.sessionMgr, err = sessions.NewManager("mem")
	if err != nil {
		log.Error("new session manager error", zap.Error(err))
		b.topicsMgr.Close()
		return nil, err
	}
	if config.ExperimentalSessionLogPath != "" {
		b.sessionLog, err = sessionstore.Open(config.ExperimentalSessionLogPath)
		if err != nil {
			_ = b.sessionMgr.Close()
			_ = b.topicsMgr.Close()
			return nil, fmt.Errorf("open experimental session log: %w", err)
		}
		if err := b.initSessionIndex(); err != nil {
			_ = b.sessionLog.Close()
			_ = b.sessionMgr.Close()
			_ = b.topicsMgr.Close()
			return nil, err
		}
		log.Warn("experimental single-node session persistence enabled; shared subscriptions and inbound QoS2 are unsupported")
	}

	if b.config.TlsPort != "" {
		tlsconfig, err := NewTLSConfig(b.config.TlsInfo)
		if err != nil {
			log.Error("new tlsConfig error", zap.Error(err))
			if b.sessionLog != nil {
				_ = b.sessionLog.Close()
			}
			_ = b.sessionMgr.Close()
			b.topicsMgr.Close()
			return nil, err
		}
		b.tlsConfig = tlsconfig
	}

	b.auth = b.config.Plugin.Auth
	b.bridgeMQ = b.config.Plugin.Bridge
	b.ctx, b.cancel = context.WithCancel(context.Background())
	b.wpool = pool.New(config.Worker)
	if config.Cluster.Listen != "" {
		c := config.Cluster
		cert, err := tls.LoadX509KeyPair(c.TLS.CertFile, c.TLS.KeyFile)
		if err != nil {
			b.Close()
			return nil, err
		}
		ca, err := os.ReadFile(c.TLS.CAFile)
		if err != nil {
			b.Close()
			return nil, err
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(ca) {
			b.Close()
			return nil, errors.New("invalid cluster CA")
		}
		gate := make(chan struct{})
		if c.Quorum.Enabled {
			b.quorumRuntime, err = openQuorumRuntime(c,
				&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots},
				&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, RootCAs: roots})
			if err != nil {
				b.Close()
				return nil, err
			}
			b.quorumRuntime.startConsumer(c.NodeID, c.ClusterID, b.publishCommittedLocal)
		}
		sender := &runtimeNodeSender{}
		if c.Reliable.Enabled {
			members := append([]string(nil), c.Reliable.MemberIDs...)
			sort.Strings(members)
			eventPath, e := canonicalLogPath(c.Reliable.EventLogPath)
			if e != nil {
				b.Close()
				return nil, e
			}
			inboxPath, e := canonicalLogPath(c.Reliable.InboxLogPath)
			if e != nil {
				b.Close()
				return nil, e
			}
			identity, e := encJson.Marshal(struct {
				Cluster, Node, Events, Inbox string
				Members                      []string
			}{c.ClusterID, c.NodeID, eventPath, inboxPath, members})
			if e != nil {
				b.Close()
				return nil, e
			}
			b.reliableRuntime, e = newBoundReliableRuntime(c.NodeID, members, eventPath, inboxPath, sender, time.Second, 0, func(m inbox.Message) error { return b.deliverClusterMessage(m.Payload) }, identity)
			if e != nil {
				b.Close()
				return nil, e
			}
		}
		var reliableMembers []string
		if c.Reliable.Enabled {
			reliableMembers = c.Reliable.MemberIDs
		}
		n, err := cluster.Start(cluster.Config{NodeID: c.NodeID, ClusterID: c.ClusterID, Secret: []byte(c.Secret), ListenAddr: c.Listen, Seeds: c.Seeds, ReliableMembers: reliableMembers,
			ServerTLS: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: roots},
			ClientTLS: &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, RootCAs: roots}, OnMessage: func(peer string, data []byte) { <-gate; b.receiveClusterMessage(peer, data) }})
		if err != nil {
			close(gate)
			b.Close()
			return nil, err
		}
		b.clusterNode = n
		if b.quorumRuntime != nil {
			b.quorumRuntime.forward, err = newQuorumForward(b.quorumRuntime, c.NodeID, c.Quorum.Peers, n.SendContext)
			if err != nil {
				close(gate)
				b.Close()
				return nil, err
			}
		}
		sender.set(n)
		close(gate)
	}

	return b, nil
}

func (b *Broker) launch(fn func()) bool {
	b.lifecycleMu.Lock()
	defer b.lifecycleMu.Unlock()
	select {
	case <-b.stopped:
		return false
	default:
	}
	b.jobs.Add(1)
	go func() { defer b.jobs.Done(); fn() }()
	return true
}

func (b *Broker) registerServer(s *http.Server) bool {
	b.lifecycleMu.Lock()
	defer b.lifecycleMu.Unlock()
	select {
	case <-b.stopped:
		return false
	default:
	}
	b.servers[s] = struct{}{}
	return true
}

func (b *Broker) trackHTTP(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.lifecycleMu.Lock()
		select {
		case <-b.stopped:
			b.lifecycleMu.Unlock()
			http.Error(w, "broker closed", http.StatusServiceUnavailable)
			return
		default:
		}
		b.jobs.Add(1)
		b.lifecycleMu.Unlock()
		defer b.jobs.Done()
		h.ServeHTTP(w, r)
	})
}

func (b *Broker) registerConnection(c net.Conn) bool {
	b.lifecycleMu.Lock()
	defer b.lifecycleMu.Unlock()
	select {
	case <-b.stopped:
		c.Close()
		return false
	default:
	}
	b.connections[c] = struct{}{}
	b.jobs.Add(1)
	return true
}

func (b *Broker) unregisterConnection(c net.Conn) {
	b.lifecycleMu.Lock()
	delete(b.connections, c)
	b.lifecycleMu.Unlock()
	c.Close()
	b.jobs.Done()
}

func (b *Broker) SubmitWork(clientId string, msg *Message) {
	b.wpool.Submit(clientId, func() { ProcessMessage(msg) })
}

func (b *Broker) registerListener(l net.Listener) bool {
	b.lifecycleMu.Lock()
	defer b.lifecycleMu.Unlock()
	select {
	case <-b.stopped:
		l.Close()
		return false
	default:
	}
	b.listeners[l] = struct{}{}
	return true
}

func (b *Broker) Close() {
	b.closeOnce.Do(func() {
		b.lifecycleMu.Lock()
		close(b.stopped)
		b.cancel()
		servers := make([]*http.Server, 0, len(b.servers))
		for s := range b.servers {
			servers = append(servers, s)
		}
		for l := range b.listeners {
			l.Close()
		}
		for c := range b.connections {
			c.Close()
		}
		b.lifecycleMu.Unlock()
		if b.quorumRuntime != nil {
			b.quorumRuntime.BeginClose()
		}
		if b.clusterNode != nil {
			b.clusterNode.WaitCallbacks()
		}
		for _, s := range servers {
			s.Close()
		}
		b.wpool.Close()
		b.jobs.Wait()
		if b.quorumRuntime != nil {
			_ = b.quorumRuntime.Close()
		}
		if b.reliableRuntime != nil {
			if err := b.reliableRuntime.Close(); err != nil {
				log.Error("close reliable runtime", zap.Error(err))
			}
		}
		for _, clients := range []*sync.Map{&b.clients} {
			clients.Range(func(_, v interface{}) bool {
				if c, ok := v.(*client); ok {
					c.Close()
				}
				return true
			})
		}
		if err := b.topicsMgr.Close(); err != nil {
			log.Error("close topic manager error", zap.Error(err))
		}
		if err := b.sessionMgr.Close(); err != nil {
			log.Error("close session manager error", zap.Error(err))
		}
		if b.sessionLog != nil {
			if err := b.sessionLog.Close(); err != nil {
				log.Error("close experimental session log", zap.Error(err))
			}
		}
		if closer, ok := b.bridgeMQ.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				log.Error("close bridge error", zap.Error(err))
			}
		}
	})
}

func (b *Broker) Start() {
	if b == nil {
		log.Error("broker is null")
		return
	}

	if b.config.HTTPPort != "" {
		b.launch(func() { InitHTTPMoniter(b) })
	}

	// listen client over tcp
	if b.config.Port != "" {
		b.launch(func() { b.StartClientListening(false) })
	}

	// listen client over unix
	if b.config.Port == "" && b.config.UnixFilePath != "" {
		b.launch(func() { b.StartUnixSocketClientListening(b.config.UnixFilePath, true) })
	}
	// listen client over windows pipe
	if b.config.Port == "" && b.config.UnixFilePath == "" && b.config.WindowsPipeName != "" {
		b.launch(func() { b.StartPipeSocketListening(b.config.WindowsPipeName, true) })
	}

	// listen for websocket
	if b.config.WsPort != "" {
		b.launch(b.StartWebsocketListening)
	}

	// listen client over tls
	if b.config.TlsPort != "" {
		b.launch(func() { b.StartClientListening(true) })
	}

}

func (b *Broker) StartWebsocketListening() {
	path := b.config.WsPath
	hp := ":" + b.config.WsPort
	log.Info("Start Websocket Listener on:", zap.String("hp", hp), zap.String("path", path))
	ws := &websocket.Server{Handler: websocket.Handler(b.wsHandler)}
	mux := http.NewServeMux()
	mux.Handle(path, ws)
	l, err := net.Listen("tcp", hp)
	if err != nil {
		log.Error("websocket listen", zap.Error(err))
		return
	}
	if !b.registerListener(l) {
		return
	}
	server := &http.Server{Handler: b.trackHTTP(mux)}
	if !b.registerServer(server) {
		l.Close()
		return
	}
	if b.config.WsTLS {
		err = server.ServeTLS(l, b.config.TlsInfo.CertFile, b.config.TlsInfo.KeyFile)
	} else {
		err = server.Serve(l)
	}
	if err != nil {
		log.Error("ListenAndServe" + err.Error())
		return
	}
}

func (b *Broker) wsHandler(ws *websocket.Conn) {
	// io.Copy(ws, ws)
	ws.PayloadType = websocket.BinaryFrame
	err := b.handleConnection(CLIENT, ws)
	if err != nil {
		ws.Close()
	}
}

func (b *Broker) StartClientListening(Tls bool) {
	var err error
	var l net.Listener
	// Retry listening indefinitely so that specifying IP addresses
	// (e.g. --host=10.0.0.217) starts working once the IP address is actually
	// configured on the interface.
	for {
		if Tls {
			hp := b.config.TlsHost + ":" + b.config.TlsPort
			l, err = tls.Listen("tcp", hp, b.tlsConfig)
			log.Info("Start TLS Listening client on ", zap.String("hp", hp))
		} else {
			hp := b.config.Host + ":" + b.config.Port
			l, err = net.Listen("tcp", hp)
			log.Info("Start Listening client on ", zap.String("hp", hp))
		}

		if err == nil {
			break // successfully listening
		}

		log.Error("Error listening on ", zap.Error(err))
		select {
		case <-time.After(time.Second):
		case <-b.stopped:
			return
		}
	}
	if !b.registerListener(l) {
		return
	}

	tmpDelay := 10 * ACCEPT_MIN_SLEEP
	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-b.stopped:
				return
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Error(
					"Temporary Client Accept Error(%v), sleeping %dms",
					zap.Error(ne),
					zap.Duration("sleeping", tmpDelay/time.Millisecond),
				)

				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else {
				log.Error("Accept error", zap.Error(err))
			}
			continue
		}

		tmpDelay = ACCEPT_MIN_SLEEP
		if !b.launch(func() {
			err := b.handleConnection(CLIENT, conn)
			if err != nil {
				conn.Close()
			}
		}) {
			conn.Close()
		}
	}
}

func (b *Broker) StartUnixSocketClientListening(socketPath string, unixSocket bool) {
	if !unixSocket {
		log.Error("unix socket listener requires unix transport")
		return
	}
	var err error
	var l net.Listener
	for {
		l, err = net.Listen("unix", socketPath)
		if err == nil {
			log.Info("Start Listening client on Unix socket", zap.String("socketPath", socketPath))
			break // successfully listening
		}

		log.Error("Error listening on ", zap.Error(err))
		select {
		case <-time.After(time.Second):
		case <-b.stopped:
			return
		}
	}
	if !b.registerListener(l) {
		return
	}

	tmpDelay := 10 * ACCEPT_MIN_SLEEP
	for {
		conn, err := l.Accept()
		if err != nil {
			select {
			case <-b.stopped:
				return
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Error(
					"Temporary Client Accept Error(%v), sleeping %dms",
					zap.Error(ne),
					zap.Duration("sleeping", tmpDelay/time.Millisecond),
				)

				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else {
				log.Error("Accept error", zap.Error(err))
			}
			continue
		}

		tmpDelay = ACCEPT_MIN_SLEEP
		if !b.launch(func() {
			err := b.handleConnection(CLIENT, conn)
			if err != nil {
				conn.Close()
			}
		}) {
			conn.Close()
		}
	}
}

func (b *Broker) DisConnClientByClientId(clientId string) {
	cli, loaded := b.clients.Load(clientId)
	if !loaded {
		return
	}
	conn, success := cli.(*client)
	if !success {
		return
	}
	conn.Close()
}

func (b *Broker) handleConnection(typ int, conn net.Conn) error {
	if !b.registerConnection(conn) {
		return errors.New("broker closed")
	}
	defer b.unregisterConnection(conn)
	// process connect packet
	packet, err := packets.ReadPacket(conn)
	if err != nil {
		return fmt.Errorf("read connect packet error: %w", err)
	}
	if packet == nil {
		return errors.New("received nil packet")
	}
	msg, ok := packet.(*packets.ConnectPacket)
	if !ok {
		return errors.New("received msg that was not Connect")
	}

	log.Info("read connect from ", getAdditionalLogFields(msg.ClientIdentifier, conn)...)

	connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	connack.ReturnCode = msg.Validate()
	if b.quorumEnabled() && (msg.WillFlag || !msg.CleanSession || typ != CLIENT) {
		connack.ReturnCode = packets.ErrRefusedNotAuthorised
	}

	if connack.ReturnCode != packets.Accepted {
		if err := connack.Write(conn); err != nil {
			return fmt.Errorf("send connack error: %w, clientID: %v, conn: %v", err, msg.ClientIdentifier, conn)
		}
		return fmt.Errorf("connect packet validate failed with connack.ReturnCode %v", connack.ReturnCode)
	}

	if typ == CLIENT && !b.CheckConnectAuth(msg.ClientIdentifier, msg.Username, string(msg.Password)) {
		connack.ReturnCode = packets.ErrRefusedNotAuthorised
		if err := connack.Write(conn); err != nil {
			return fmt.Errorf("send connack error: %w, clientID: %v, conn: %v", err, msg.ClientIdentifier, conn)
		}
		return fmt.Errorf("connect packet CheckConnectAuth failed with connack.ReturnCode %v", connack.ReturnCode)
	}

	willmsg := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	if msg.WillFlag {
		willmsg.Qos = msg.WillQos
		willmsg.TopicName = msg.WillTopic
		willmsg.Retain = msg.WillRetain
		willmsg.Payload = msg.WillMessage
		willmsg.Dup = msg.Dup
	} else {
		willmsg = nil
	}
	info := info{
		clientID:  msg.ClientIdentifier,
		username:  msg.Username,
		keepalive: msg.Keepalive,
		willMsg:   willmsg,
	}

	c := &client{
		typ:    typ,
		broker: b,
		conn:   conn,
		info:   info,
	}

	c.init()
	var endTakeover func()
	if typ == CLIENT {
		if old, ok := b.clients.Load(msg.ClientIdentifier); ok {
			if previous, ok := old.(*client); ok {
				endTakeover = previous.beginTakeover()
			}
		}
	}
	defer func() {
		if endTakeover != nil {
			endTakeover()
		}
	}()
	unlockConnect := b.lockClientConnect(msg.ClientIdentifier)
	locked := true
	var oldNotify func()
	defer func() {
		if locked {
			unlockConnect()
		}
		if oldNotify != nil {
			oldNotify()
		}
	}()

	if !msg.CleanSession {
		_, err = b.sessionMgr.Get(msg.ClientIdentifier)
		connack.SessionPresent = err == nil
		if b.sessionLog != nil && typ == CLIENT {
			_, exists, _, _, readErr := b.sessionLog.Inspect(msg.ClientIdentifier, 0)
			if readErr != nil {
				return readErr
			}
			connack.SessionPresent = exists
		}
	}
	committedConnack := false
	if b.sessionLog != nil && typ == CLIENT {
		rollback, prepareErr := c.prepareSessionLog(msg.CleanSession)
		defer func() {
			if !committedConnack && rollback != nil {
				if err := rollback(); err != nil {
					log.Error("rollback failed CONNACK session", zap.Error(err))
				}
			}
		}()
		if prepareErr != nil {
			return prepareErr
		}
	}
	if err := conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		return fmt.Errorf("set connack write deadline: %w", err)
	}
	err = connack.Write(conn)
	_ = conn.SetWriteDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("send connack error: %w, clientID: %v, conn: %v", err, msg.ClientIdentifier, conn)
	}
	committedConnack = true
	if typ == CLIENT {
		if old, ok := b.clients.Load(msg.ClientIdentifier); ok {
			if ol, ok := old.(*client); ok {
				oldNotify = ol.detach()
			}
		}
	}
	if msg.CleanSession {
		if b.sessionLog != nil && typ == CLIENT {
			b.deleteSessionIndex(msg.ClientIdentifier)
		}
		b.sessionMgr.Del(msg.ClientIdentifier)
		c.session = &sessions.Session{}
		err = c.session.Init(msg)
	} else {
		err = b.getSession(c, msg, connack)
	}
	if err != nil {
		return fmt.Errorf("get session error: %w, clientID: %v, conn: %v", err, msg.ClientIdentifier, conn)
	}
	if err := c.loadSessionLog(); err != nil {
		return err
	}
	if err := c.restoreSubscriptions(); err != nil {
		cleanup := c.detach()
		unlockConnect()
		locked = false
		if cleanup != nil {
			cleanup()
		}
		return fmt.Errorf("restore subscriptions: %w", err)
	}
	var recovered map[uint16]sessions.OutboundMessage
	c.outboundOwner, recovered = c.session.ClaimOutbound()
	for id, entry := range recovered {
		status := Publish
		if entry.AwaitingPubcomp {
			status = Pubrel
		}
		c.inflight[id] = &inflightElem{status: status, packet: entry.Packet, timestamp: time.Now().Unix()}
	}

	if typ == CLIENT {
		b.clients.Store(c.info.clientID, c)
	}
	unlockConnect()
	locked = false
	if endTakeover != nil {
		endTakeover()
		endTakeover = nil
	}
	if oldNotify != nil {
		oldNotify()
		oldNotify = nil
	}
	if typ == CLIENT {
		pubPack := PubPacket{}
		if willmsg != nil {
			pubPack.TopicName, pubPack.Payload = willmsg.TopicName, willmsg.Payload
		}
		b.OnlineOfflineNotification(Info{ClientID: info.clientID, Username: info.username, Keepalive: info.keepalive, WillMsg: pubPack}, true, c.lastMsgTime)
		b.Publish(&bridge.Elements{ClientID: msg.ClientIdentifier, Username: msg.Username, Action: bridge.Connect, Timestamp: time.Now().Unix()})
	}
	recoveredIDs := make([]uint16, 0, len(recovered))
	for id := range recovered {
		recoveredIDs = append(recoveredIDs, id)
	}
	if c.durable() {
		recoveredIDs = c.durableReplayIDs
	}
	for _, id := range recoveredIDs {
		entry := recovered[id]
		if entry.AwaitingPubcomp {
			rel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
			rel.MessageID = id
			if err := c.WriterPacket(rel); err != nil {
				c.Close()
				return err
			}
		} else {
			packet := *entry.Packet
			packet.Dup = true
			if err := c.WriterPacket(&packet); err != nil {
				c.Close()
				return err
			}
		}
	}
	if len(recovered) > 0 {
		c.ensureRetryTimer()
	}
	if c.durable() {
		unlock := b.lockClientConnect(c.info.clientID)
		current, ok := b.clients.Load(c.info.clientID)
		if !ok || current != c {
			unlock()
			c.Close()
			return net.ErrClosed
		}
		c.outboundSendMu.Lock()
		c.durableReady = true
		err := c.pumpSessionQueueLocked()
		c.outboundSendMu.Unlock()
		unlock()
		if err != nil {
			c.Close()
			return err
		}
	}

	c.readLoop()
	return nil
}

func (b *Broker) removeClient(c *client) {
	clientId := c.info.clientID
	typ := c.typ
	switch typ {
	case CLIENT:
		b.clients.CompareAndDelete(clientId, c)
	}
}

func (b *Broker) PublishMessage(packet *packets.PublishPacket) error {
	if b.quorumEnabled() {
		return b.quorumRuntime.publish(packet)
	}
	if packet.Retain {
		if err := b.topicsMgr.Retain(packet); err != nil {
			log.Error("retain local publish", zap.Error(err))
		}
	}
	if err := b.forwardMessage(packet); err != nil {
		log.Error("forward publish", zap.Error(err))
		return err
	}
	return b.publishLocal(packet)
}

type clusterPublish struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
}

func (b *Broker) broadcastClusterMessage(packet *packets.PublishPacket) {
	if b.clusterNode == nil {
		return
	}
	data, err := encJson.Marshal(clusterPublish{packet.TopicName, packet.Payload, packet.Qos, packet.Retain})
	if err != nil {
		log.Error("encode cluster publish", zap.Error(err))
		return
	}
	for peer, err := range b.clusterNode.Broadcast(data) {
		log.Error("cluster publish failed", zap.String("peer", peer), zap.Error(err))
	}
}

func (b *Broker) receiveClusterMessage(peer string, data []byte) {
	if b.quorumEnabled() {
		if b.quorumRuntime != nil && b.quorumRuntime.forward != nil {
			b.quorumRuntime.forward.handle(peer, data)
		}
		return
	}
	if b.reliableRuntime != nil {
		handled, err := b.reliableRuntime.handle(peer, data)
		if err != nil {
			log.Error("reliable cluster frame", zap.Error(err))
		}
		if handled {
			return
		}
		// Reliable mode must not accept unacknowledged legacy publications.
		return
	}
	_ = b.deliverClusterMessage(data)
}

func (b *Broker) deliverClusterMessage(data []byte) error {
	if b.quorumEnabled() {
		return ErrQuorumRoutingUnavailable
	}
	var msg clusterPublish
	if err := encJson.Unmarshal(data, &msg); err != nil || msg.Topic == "" || msg.QoS > 2 {
		return errors.New("invalid cluster publication")
	}
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Payload, p.Qos, p.Retain = msg.Topic, msg.Payload, msg.QoS, msg.Retain
	if p.Retain {
		if err := b.topicsMgr.Retain(p); err != nil {
			log.Error("retain cluster publish", zap.Error(err))
			if b.reliableRuntime != nil {
				return err
			}
		}
	}
	return b.publishLocal(p)
}

func (b *Broker) publishLocal(packet *packets.PublishPacket) error {
	if b.quorumEnabled() {
		return ErrQuorumRoutingUnavailable
	}
	return b.publishCommittedLocal(packet)
}

func (b *Broker) publishCommittedLocal(packet *packets.PublishPacket) error {
	if err := b.routeDurable(packet); err != nil {
		return err
	}
	var subs []interface{}
	var qoss []byte
	b.mu.Lock()
	err := b.topicsMgr.Subscribers([]byte(packet.TopicName), packet.Qos, &subs, &qoss)
	b.mu.Unlock()
	if err != nil {
		log.Error("search sub client error", zap.Error(err))
		return err
	}

	var deliveryErr error
	deliver := func(s *subscription) (bool, error) {
		unlock := b.lockClientConnect(s.client.info.clientID)
		var interruptedClient *client
		defer func() {
			unlock()
			if interruptedClient != nil {
				interruptedClient.Close()
			}
		}()
		value, ok := b.clients.Load(s.client.info.clientID)
		if !ok {
			return false, nil
		}
		current, ok := value.(*client)
		if ok && current.durable() && !s.share {
			return true, nil
		}
		if !ok || current.isDisconnected() {
			return false, nil
		}
		key := s.topic
		if s.share {
			key = "$share/" + s.groupName + "/" + s.topic
		}
		current.subMapMu.RLock()
		active := current.subMap[key]
		if active == nil || active.share != s.share || active.groupName != s.groupName || active.topic != s.topic || active.generation != s.generation {
			current.subMapMu.RUnlock()
			return false, nil
		}
		qos := active.qos
		current.subMapMu.RUnlock()
		if qos > packet.Qos {
			qos = packet.Qos
		}
		err := publish(active, packet, qos)
		if errors.Is(err, errTakeoverWrite) {
			if errors.Is(err, errTakeoverWriteStarted) {
				interruptedClient = current
			}
			log.Warn("subscription delivery interrupted by takeover", zap.String("ClientID", current.info.clientID), zap.Error(err))
			return !(errors.Is(err, errWriteNotStarted) || (s.share && errors.Is(err, errWriteZeroBytes))), nil
		}
		if errors.Is(err, net.ErrClosed) && current.isDisconnected() {
			return !errors.Is(err, errWriteNotStarted), nil
		}
		return true, err
	}
	type shareKey struct{ group, filter string }
	groups := make(map[shareKey][]int)
	for i, sub := range subs {
		s, ok := sub.(*subscription)
		if ok {
			if s.share {
				key := shareKey{s.groupName, s.topic}
				groups[key] = append(groups[key], i)
			} else {
				if _, err := deliver(s); err != nil {
					log.Error("deliver subscription", zap.Error(err), zap.String("ClientID", s.client.info.clientID))
					deliveryErr = errors.Join(deliveryErr, err)
				}
			}
		}
	}
	for _, candidates := range groups {
		start := int(time.Now().UnixNano() % int64(len(candidates)))
		err := deliverSharedCandidates(candidates, start, func(i int) (bool, error) {
			s := subs[i].(*subscription)
			delivered, err := deliver(s)
			if err != nil {
				log.Error("deliver shared subscription", zap.Error(err), zap.String("ClientID", s.client.info.clientID))
			}
			return delivered, err
		})
		deliveryErr = errors.Join(deliveryErr, err)
	}
	return deliveryErr
}

func deliverSharedCandidates(candidates []int, start int, deliver func(int) (bool, error)) error {
	var result error
	for n := range candidates {
		delivered, err := deliver(candidates[(start+n)%len(candidates)])
		result = errors.Join(result, err)
		if delivered {
			break
		}
	}
	return result
}

func (b *Broker) PublishMessageByClientId(packet *packets.PublishPacket, clientId string) error {
	if b.quorumEnabled() {
		return ErrQuorumRoutingUnavailable
	}
	cli, loaded := b.clients.Load(clientId)
	if !loaded {
		return fmt.Errorf("clientId %s not connected", clientId)
	}
	conn, success := cli.(*client)
	if !success {
		return fmt.Errorf("clientId %s loaded fail", clientId)
	}
	return conn.WriterPacket(packet)
}

type OnlineOfflineMsg struct {
	ClientID    string `json:"clientID"`
	Online      bool   `json:"online"`
	Timestamp   string `json:"timestamp"`
	ClientInfo  Info   `json:"info"`
	LastMsgTime int64  `json:"lastMsg"`
}

func (b *Broker) OnlineOfflineNotification(info Info, online bool, lastMsg int64) {
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = "$SYS/broker/connection/clients/" + info.ClientID
	packet.Qos = 0

	msg := OnlineOfflineMsg{
		ClientID:    info.ClientID,
		Online:      online,
		Timestamp:   time.Now().UTC().Format(time.RFC3339),
		ClientInfo:  info,
		LastMsgTime: lastMsg,
	}

	if b, err := encJson.Marshal(msg); err != nil {
		// This is a TERRIBLE situation, falling back to legacy format to not break API Contract
		packet.Payload = []byte(fmt.Sprintf(`{"clientID":"%s","online":%v,"timestamp":"%s"}`, info.ClientID, online, time.Now().UTC().Format(time.RFC3339)))
	} else {
		packet.Payload = b
	}

	b.PublishMessage(packet)
}
