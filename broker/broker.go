package broker

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"hmq/broker/lib/sessions"
	"hmq/broker/lib/topics"
	"hmq/broker/packets"
	"hmq/plugins/auth"
	"hmq/plugins/bridge"
	"hmq/pool"

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
	id          string
	mu          sync.Mutex
	config      *Config
	tlsConfig   *tls.Config
	wpool       *pool.WorkerPool
	clients     sync.Map
	routes      sync.Map
	remotes     sync.Map
	nodes       map[string]interface{}
	clusterPool chan *Message
	topicsMgr   *topics.Manager
	sessionMgr  *sessions.Manager
	auth        auth.Auth
	bridgeMQ    bridge.BridgeMQ
}

// CreateNewMessage creates a new pointer to Message struct
func CreateNewMessage(client *client, packet packets.ControlPacket) (msg *Message) {

	msg = &Message{
		client: client,
		packet: packet,
	}

	return
}

func newMessagePool() []chan *Message {
	msgPool := make([]chan *Message, 0)
	for i := 0; i < MessagePoolNum; i++ {
		ch := make(chan *Message, MessagePoolMessageNum)
		msgPool = append(msgPool, ch)
	}
	return msgPool
}

func getAdditionalLogFields(clientIdentifier string, conn net.Conn, additionalFields ...zapcore.Field) []zapcore.Field {
	var wsConn *websocket.Conn = nil
	var wsEnabled bool
	var result []zapcore.Field

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
	if !wsEnabled && conn != nil && conn.RemoteAddr() != nil {
		result = append(result, zap.Stringer("addr", conn.RemoteAddr()))
	} else if wsEnabled && wsConn != nil && wsConn.Request() != nil {
		result = append(result, zap.String("addr", wsConn.Request().RemoteAddr))
	}

	return result
}

func NewBroker(config *Config) (*Broker, error) {
	if config == nil {
		config = DefaultConfig
	}

	b := &Broker{
		id:          GenUniqueId(),
		config:      config,
		wpool:       pool.New(config.Worker),
		nodes:       make(map[string]interface{}),
		clusterPool: make(chan *Message),
	}

	var err error
	b.topicsMgr, err = topics.NewManager("mem")
	if err != nil {
		log.Error("new topic manager error", zap.Error(err))
		return nil, err
	}

	b.sessionMgr, err = sessions.NewManager("mem")
	if err != nil {
		log.Error("new session manager error", zap.Error(err))
		return nil, err
	}

	if b.config.TlsPort != "" {
		tlsconfig, err := NewTLSConfig(b.config.TlsInfo)
		if err != nil {
			log.Error("new tlsConfig error", zap.Error(err))
			return nil, err
		}
		b.tlsConfig = tlsconfig
	}

	b.auth = b.config.Plugin.Auth
	b.bridgeMQ = b.config.Plugin.Bridge

	return b, nil
}

func (broker *Broker) SubmitWork(clientId string, msg *Message, ctx context.Context, cancel context.CancelFunc) {
	if broker.wpool == nil {
		broker.wpool = pool.New(broker.config.Worker)
	}

	if msg.client.category == CLUSTER {
		broker.clusterPool <- msg
	} else {

		// creates function
		handleMessage := func() {
			ProcessClientMessage(msg, ctx, cancel)
		}

		// Submit the client to worker pool
		broker.wpool.Submit(clientId, handleMessage)
	}
}

func (broker *Broker) Start() {
	if broker == nil {
		log.Error("broker is null")
		return
	}

	if broker.config.HTTPPort != "" {
		go InitHTTPMoniter(broker)
	}

	//listen client over tcp
	if broker.config.Port != "" {
		go broker.StartClientListening(false)
	}

	//listen for cluster
	if broker.config.Cluster.Port != "" {
		go broker.StartClusterListening()
	}

	//listen for websocket
	if broker.config.WsPort != "" {
		go broker.StartWebsocketListening()
	}

	//listen client over tls
	if broker.config.TlsPort != "" {
		go broker.StartClientListening(true)
	}

	//connect on other node in cluster
	if broker.config.Router != "" {
		go broker.processClusterInfo()
		broker.ConnectToDiscovery()
	}

}

func (broker *Broker) StartWebsocketListening() {
	path := broker.config.WsPath
	hp := ":" + broker.config.WsPort
	log.Info("Start Websocket Listener on:", zap.String("hp", hp), zap.String("path", path))
	ws := &websocket.Server{Handler: websocket.Handler(broker.wsHandler)}
	mux := http.NewServeMux()
	mux.Handle(path, ws)
	var err error
	if broker.config.WsTLS {
		err = http.ListenAndServeTLS(hp, broker.config.TlsInfo.CertFile, broker.config.TlsInfo.KeyFile, mux)
	} else {
		err = http.ListenAndServe(hp, mux)
	}
	if err != nil {
		log.Error("ListenAndServe" + err.Error())
		return
	}
}

func (broker *Broker) wsHandler(ws *websocket.Conn) {
	// io.Copy(ws, ws)
	ws.PayloadType = websocket.BinaryFrame
	broker.handleConnection(CLIENT, ws)
}

// startSecureListener starts a TCP listener with TLS enabled
func (broker *Broker) startSecureListener() (l net.Listener, err error) {
	hp := broker.config.TlsHost + ":" + broker.config.TlsPort
	l, err = tls.Listen("tcp", hp, broker.tlsConfig)
	log.Info("Start TLS Listening client on ", zap.String("hp", hp))

	return l, err
}

// startInsecureListener starts a TCP listener without TLS enabled
func (broker *Broker) startInsecureListener() (l net.Listener, err error) {
	hp := broker.config.Host + ":" + broker.config.Port
	l, err = net.Listen("tcp", hp)
	log.Info("Start Listening client on ", zap.String("hp", hp))

	return l, err
}

// handleAcceptError handles the error that occurs when the listener fails to accept a new client
func handleAcceptError(delay time.Duration, err error) (bShouldContinue bool) {
	if err != nil {

		// Checks error type
		ne, ok := err.(net.Error)

		// Handle error as needed
		if ok && ne.Temporary() {
			log.Error("Temporary Client Accept Error(%v), sleeping %dms", zap.Error(ne),
				zap.Duration("sleeping", delay/time.Millisecond))

			time.Sleep(delay)
			delay *= 2
			if delay > ACCEPT_MAX_SLEEP {
				delay = ACCEPT_MAX_SLEEP
			}
		} else {
			log.Error("Accept error", zap.Error(err))
		}

		bShouldContinue = true
		return
	}

	bShouldContinue = false
	return
}

func (broker *Broker) StartClientListening(Tls bool) {
	var err error
	var l net.Listener

	// Retry listening indefinitely so that specifying IP addresses
	// (e.g. --host=10.0.0.217) starts working once the IP address is actually
	// configured on the interface.

	for {
		if Tls {
			l, err = broker.startSecureListener()
		} else {
			l, err = broker.startInsecureListener()
		}

		if err == nil {
			break // successfully listening
		}

		log.Error("Error listening on ", zap.Error(err))
		time.Sleep(1 * time.Second)
	}

	tmpDelay := 10 * ACCEPT_MIN_SLEEP

	for {
		// Accepts client and handle error if any
		conn, err := l.Accept()
		if handleAcceptError(tmpDelay, err) {
			continue
		}

		// set delay time
		tmpDelay = ACCEPT_MIN_SLEEP

		// Starts connection handling
		go broker.handleConnection(CLIENT, conn)
	}
}

func (broker *Broker) StartClusterListening() {
	var hp string = broker.config.Cluster.Host + ":" + broker.config.Cluster.Port
	log.Info("Start Listening cluster on ", zap.String("hp", hp))

	l, e := net.Listen("tcp", hp)
	if e != nil {
		log.Error("Error listening on", zap.Error(e))
		return
	}

	tmpDelay := 10 * ACCEPT_MIN_SLEEP
	for {
		conn, err := l.Accept()
		if err != nil {
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

		go broker.handleConnection(ROUTER, conn)
	}
}

func (broker *Broker) DisConnClientByClientId(clientId string) {
	cli, loaded := broker.clients.LoadAndDelete(clientId)
	if !loaded {
		return
	}
	conn, success := cli.(*client)
	if !success {
		return
	}
	conn.Close(context.WithCancel(context.Background()))
}

func (broker *Broker) handleConnection(clientType int, conn net.Conn) {
	// Process connect packet
	packet, err := packets.ReadPacket(conn)
	if err != nil {
		log.Error("read connect packet error", zap.Error(err))
		conn.Close()
		return
	}

	if packet == nil {
		log.Error("received nil packet")
		return
	}

	// Perform field validation
	if !validatePacketFields(packet) {

		// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.pdf
		// Page 14
		//
		// If a Server or Client receives a Control Packet
		// containing ill-formed UTF-8 it MUST close the Network Connection

		_ = conn.Close()

		// Update client status
		//clientInstance.status = Disconnected

		log.Error("Client disconnected due to malformed packet")

		return
	}

	msg, ok := packet.(*packets.ConnectPacket)
	if !ok {
		/*
		* The Server MUST process a second CONNECT Packet sent from a Client as a
		* protocol violation and disconnect the Client
		 */
		log.Error("received msg that was not Connect")
		conn.Close()
		return
	}

	// Handles connect packet
	conNack := broker.processConnect(msg, conn, clientType)
	if conNack == nil {
		return
	}

	log.Info("read connect from ", getAdditionalLogFields(msg.ClientIdentifier, conn)...)

	// Generates a new will message as needed
	willMsg := broker.generateWillMsg(msg)

	// setup client information struct
	info := info{
		clientID:  msg.ClientIdentifier,
		username:  msg.Username,
		password:  msg.Password,
		keepalive: msg.Keepalive,
		willMsg:   willMsg,
	}

	clientInstance := &client{
		category: clientType,
		broker:   broker,
		conn:     conn,
		info:     info,
	}

	// Setup client
	clientInstance.init()

	// checks if client needs to recover a previous session
	if err := broker.getSession(clientInstance, msg, conNack); err != nil {
		log.Error("get session error", getAdditionalLogFields(clientInstance.info.clientID, conn, zap.Error(err))...)
		return
	}

	cid := clientInstance.info.clientID

	var exists bool
	var old interface{}

	switch clientType {
	case CLIENT:

		// Checks if a specified client exists, and store it as needed
		old, exists = broker.clients.Load(cid)
		if exists {
			if ol, ok := old.(*client); ok {
				log.Warn("client exists, close old client", getAdditionalLogFields(ol.info.clientID, ol.conn)...)
				ol.Close(context.WithCancel(context.Background()))
			}
		}

		// Insert client into a list of connected clients
		broker.clients.Store(cid, clientInstance)

		// Updates client status
		broker.OnlineOfflineNotification(cid, true)
		{
			broker.Publish(&bridge.Elements{
				ClientID:  msg.ClientIdentifier,
				Username:  msg.Username,
				Action:    bridge.Connect,
				Timestamp: time.Now().Unix(),
			})
		}
	case ROUTER:
		old, exists = broker.routes.Load(cid)
		if exists {
			if ol, ok := old.(*client); ok {
				log.Warn("router exists, close old router", getAdditionalLogFields(ol.info.clientID, ol.conn)...)
				ol.Close(context.WithCancel(context.Background()))
			}
		}
		broker.routes.Store(cid, clientInstance)
	}

	// start reading loop
	clientInstance.readLoop(context.WithCancel(context.Background()))
}

func (broker *Broker) ConnectToDiscovery() {
	var conn net.Conn
	var err error
	var tempDelay time.Duration = 0
	for {
		conn, err = net.Dial("tcp", broker.config.Router)
		if err != nil {
			log.Error("Error trying to connect to route", zap.Error(err))
			log.Debug("Connect to route timeout, retry...")

			if 0 == tempDelay {
				tempDelay = 1 * time.Second
			} else {
				tempDelay *= 2
			}

			if max := 20 * time.Second; tempDelay > max {
				tempDelay = max
			}
			time.Sleep(tempDelay)
			continue
		}
		break
	}
	log.Debug("connect to router success", zap.String("Router", broker.config.Router))

	cid := broker.id
	info := info{
		clientID:  cid,
		keepalive: 60,
	}

	c := &client{
		category: CLUSTER,
		broker:   broker,
		conn:     conn,
		info:     info,
	}

	c.init()

	c.SendConnect(context.WithCancel(context.Background()))
	c.SendInfo(context.WithCancel(context.Background()))

	go c.readLoop(context.WithCancel(context.Background()))
	go c.StartPing(context.WithCancel(context.Background()))
}

func (broker *Broker) processClusterInfo() {
	for {
		msg, ok := <-broker.clusterPool
		if !ok {
			log.Error("read message from cluster channel error")
			return
		}

		ctx, cancel := context.WithCancel(context.Background())
		ProcessClientMessage(msg, ctx, cancel)
	}

}

func (broker *Broker) connectRouter(id, addr string) {
	var conn net.Conn
	var err error
	var timeDelay time.Duration = 0
	retryTimes := 0
	max := 32 * time.Second
	for {

		if !broker.checkNodeExist(id, addr) {
			return
		}

		conn, err = net.Dial("tcp", addr)
		if err != nil {
			log.Error("Error trying to connect to route", zap.Error(err))

			if retryTimes > 50 {
				return
			}

			log.Debug("Connect to route timeout, retry...")

			if 0 == timeDelay {
				timeDelay = 1 * time.Second
			} else {
				timeDelay *= 2
			}

			if timeDelay > max {
				timeDelay = max
			}
			time.Sleep(timeDelay)
			retryTimes++
			continue
		}
		break
	}
	route := route{
		remoteID:  id,
		remoteUrl: addr,
	}
	cid := GenUniqueId()

	info := info{
		clientID:  cid,
		keepalive: 60,
	}

	c := &client{
		broker:   broker,
		category: REMOTE,
		conn:     conn,
		route:    route,
		info:     info,
	}
	c.init()
	broker.remotes.Store(cid, c)

	c.SendConnect(context.WithCancel(context.Background()))

	go c.readLoop(context.WithCancel(context.Background()))
	go c.StartPing(context.WithCancel(context.Background()))

}

func (broker *Broker) checkNodeExist(id, url string) bool {
	if id == broker.id {
		return false
	}

	for k, v := range broker.nodes {
		if k == id {
			return true
		}

		//skip
		l, ok := v.(string)
		if ok {
			if url == l {
				return true
			}
		}

	}
	return false
}

func (broker *Broker) CheckRemoteExist(remoteID, url string) bool {
	exists := false
	broker.remotes.Range(func(key, value interface{}) bool {
		v, ok := value.(*client)
		if ok {
			if v.route.remoteUrl == url {
				v.route.remoteID = remoteID
				exists = true
				return false
			}
		}
		return true
	})
	return exists
}

func (broker *Broker) SendLocalSubsToRouter(c *client) {
	subInfo := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	broker.clients.Range(func(key, value interface{}) bool {
		client, ok := value.(*client)
		if !ok {
			return true
		}

		client.subMapMu.RLock()
		defer client.subMapMu.RUnlock()

		subs := client.subMap
		for _, sub := range subs {
			subInfo.Topics = append(subInfo.Topics, sub.topic)
			subInfo.Qoss = append(subInfo.Qoss, sub.qos)
		}

		return true
	})
	if len(subInfo.Topics) > 0 {
		ctx, cancel := context.WithCancel(context.Background())
		if err := c.WriterPacket(subInfo, ctx, cancel); err != nil {
			log.Error("Send localsubs To Router error", zap.Error(err))
		}
	}
}

func (broker *Broker) BroadcastInfoMessage(remoteID string, msg *packets.PublishPacket) {
	broker.routes.Range(func(key, value interface{}) bool {
		if r, ok := value.(*client); ok {
			if r.route.remoteID == remoteID {
				return true
			}
			ctx, cancel := context.WithCancel(context.Background())
			r.WriterPacket(msg, ctx, cancel)
		}
		return true
	})
}

func (broker *Broker) BroadcastSubOrUnsubMessage(packet packets.ControlPacket) {

	broker.routes.Range(func(key, value interface{}) bool {
		if r, ok := value.(*client); ok {
			ctx, cancel := context.WithCancel(context.Background())
			r.WriterPacket(packet, ctx, cancel)
		}
		return true
	})
}

func (broker *Broker) removeClient(c *client) {
	clientId := c.info.clientID
	typ := c.category
	switch typ {
	case CLIENT:
		broker.clients.Delete(clientId)
	case ROUTER:
		broker.routes.Delete(clientId)
	case REMOTE:
		broker.remotes.Delete(clientId)
	}
}

func (broker *Broker) PublishMessage(packet *packets.PublishPacket) {
	var subs []interface{}
	var qoss []byte

	broker.mu.Lock()
	err := broker.topicsMgr.Subscribers([]byte(packet.TopicName), packet.Qos, &subs, &qoss)
	broker.mu.Unlock()
	if err != nil {
		log.Error("search sub client error", zap.Error(err))
		return
	}

	for _, sub := range subs {
		s, ok := sub.(*subscription)
		if ok {
			ctx, cancel := context.WithCancel(context.Background())
			if err := s.client.WriterPacket(packet, ctx, cancel); err != nil {
				log.Error("write message error", zap.Error(err))
			}
		}
	}
}

func (broker *Broker) PublishMessageByClientId(packet *packets.PublishPacket, clientId string) error {
	cli, loaded := broker.clients.LoadAndDelete(clientId)
	if !loaded {
		return fmt.Errorf("clientId %s not connected", clientId)
	}
	conn, success := cli.(*client)
	if !success {
		return fmt.Errorf("clientId %s loaded fail", clientId)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return conn.WriterPacket(packet, ctx, cancel)
}

func (broker *Broker) BroadcastUnSubscribe(topicsToUnSubscribeFrom []string) {
	if len(topicsToUnSubscribeFrom) == 0 {
		return
	}

	unsub := packets.NewControlPacket(packets.Unsubscribe).(*packets.UnsubscribePacket)
	unsub.Topics = append(unsub.Topics, topicsToUnSubscribeFrom...)
	broker.BroadcastSubOrUnsubMessage(unsub)
}

// OnlineOfflineNotification updates the online/offline status of a client
func (broker *Broker) OnlineOfflineNotification(clientID string, online bool) {
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = "$SYS/broker/connection/clients/" + clientID
	packet.Qos = 0
	packet.Payload = []byte(fmt.Sprintf(`{"clientID":"%s","online":%v,"timestamp":"%s"}`, clientID, online, time.Now().UTC().Format(time.RFC3339)))

	broker.PublishMessage(packet)
}

// generateWillMsg
func (broker *Broker) generateWillMsg(packet *packets.ConnectPacket) (will *packets.PublishPacket) {
	var willMsg *packets.PublishPacket

	if packet.WillFlag {
		willMsg = packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		willMsg.Qos = packet.WillQos
		willMsg.TopicName = packet.WillTopic
		willMsg.Retain = packet.WillRetain
		willMsg.Payload = packet.WillMessage
		willMsg.Dup = packet.Dup
	}

	return willMsg
}

// processConnect handles connect packet and returns the built connack packet
func (broker *Broker) processConnect(packet *packets.ConnectPacket, conn net.Conn, clientType int) (connAckPacket *packets.ConnackPacket) {

	/*
	* After a Network Connection is established by a Client to a Server, the first Packet
	* sent from the Client to 365 the Server MUST be a CONNECT Packet
	 */

	// build CONNACK packet
	conNack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	conNack.SessionPresent = packet.CleanSession
	conNack.ReturnCode = packet.Validate()

	// Check if packet have been validated
	if conNack.ReturnCode != packets.Accepted {
		defer func() {
			_ = conn.Close()
		}()

		// Send packet back to client
		if err := conNack.Write(conn); err != nil {
			log.Error(ERR_SEND_CONNACK, getAdditionalLogFields(packet.ClientIdentifier, conn, zap.Error(err))...)
		}

		return nil
	}

	// Handles authorization
	if clientType == CLIENT &&
		!broker.CheckConnectAuth(packet.ClientIdentifier, packet.Username, string(packet.Password)) {
		conNack.ReturnCode = packets.ErrRefusedNotAuthorised

		defer func() {
			_ = conn.Close()
		}()

		if err := conNack.Write(conn); err != nil {
			log.Error(ERR_SEND_CONNACK, getAdditionalLogFields(packet.ClientIdentifier, conn, zap.Error(err))...)
		}

		return nil
	}

	// Send connack back to client
	if err := conNack.Write(conn); err != nil {
		log.Error(ERR_SEND_CONNACK, getAdditionalLogFields(packet.ClientIdentifier, conn, zap.Error(err))...)
		return nil
	}

	return conNack
}

func (c *client) processConnack() {

}

func (c *client) processDisconnect() {

}

func (c *client) processPingReq() {

}

func (c *client) processPingResp() {

}

func (c *client) processPublish() {

}

func (c *client) processPuback() {

}

func (c *client) processPubcomp() {

}

func (c *client) processPubrec() {

}

func (c *client) processPubrel() {

}

func (c *client) processSubscribe() {

}

func (c *client) processSuback() {

}

func (c *client) processUnsubscribe() {

}

func (c *client) processUnsuback() {

}
