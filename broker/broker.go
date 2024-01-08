package broker

import (
	"crypto/tls"
	encJson "encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/fhmq/hmq/broker/lib/sessions"
	"github.com/fhmq/hmq/broker/lib/topics"
	"github.com/fhmq/hmq/plugins/auth"
	"github.com/fhmq/hmq/plugins/bridge"
	"github.com/fhmq/hmq/pool"

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

func (b *Broker) SubmitWork(clientId string, msg *Message) {
	if b.wpool == nil {
		b.wpool = pool.New(b.config.Worker)
	}

	if msg.client.typ == CLUSTER {
		b.clusterPool <- msg
	} else {
		b.wpool.Submit(clientId, func() {
			ProcessMessage(msg)
		})
	}

}

func (b *Broker) Start() {
	if b == nil {
		log.Error("broker is null")
		return
	}

	if b.config.HTTPPort != "" {
		go InitHTTPMoniter(b)
	}

	//listen client over tcp
	if b.config.Port != "" {
		go b.StartClientListening(false)
	}

	//listen for cluster
	if b.config.Cluster.Port != "" {
		go b.StartClusterListening()
	}

	//listen for websocket
	if b.config.WsPort != "" {
		go b.StartWebsocketListening()
	}

	//listen client over tls
	if b.config.TlsPort != "" {
		go b.StartClientListening(true)
	}

	//connect on other node in cluster
	if b.config.Router != "" {
		go b.processClusterInfo()
		b.ConnectToDiscovery()
	}

}

func (b *Broker) StartWebsocketListening() {
	path := b.config.WsPath
	hp := ":" + b.config.WsPort
	log.Info("Start Websocket Listener on:", zap.String("hp", hp), zap.String("path", path))
	ws := &websocket.Server{Handler: websocket.Handler(b.wsHandler)}
	mux := http.NewServeMux()
	mux.Handle(path, ws)
	var err error
	if b.config.WsTLS {
		err = http.ListenAndServeTLS(hp, b.config.TlsInfo.CertFile, b.config.TlsInfo.KeyFile, mux)
	} else {
		err = http.ListenAndServe(hp, mux)
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
		time.Sleep(1 * time.Second)
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
		go func() {
			err := b.handleConnection(CLIENT, conn)
			if err != nil {
				conn.Close()
			}
		}()
	}
}

func (b *Broker) StartClusterListening() {
	var hp string = b.config.Cluster.Host + ":" + b.config.Cluster.Port
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

		go func() {
			err := b.handleConnection(ROUTER, conn)
			if err != nil {
				conn.Close()
			}
		}()
	}
}

func (b *Broker) DisConnClientByClientId(clientId string) {
	cli, loaded := b.clients.LoadAndDelete(clientId)
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
	//process connect packet
	packet, err := packets.ReadPacket(conn)
	if err != nil {
		return errors.New(fmt.Sprintf("read connect packet error:%v", err))
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
	connack.SessionPresent = msg.CleanSession
	connack.ReturnCode = msg.Validate()

	if connack.ReturnCode != packets.Accepted {
		if err := connack.Write(conn); err != nil {
			return fmt.Errorf("send connack error:%v,clientID:%v,conn:%v", err, msg.ClientIdentifier, conn)
		}
		return fmt.Errorf("connect packet validate failed with connack.ReturnCode%v", connack.ReturnCode)
	}

	if typ == CLIENT && !b.CheckConnectAuth(msg.ClientIdentifier, msg.Username, string(msg.Password)) {
		connack.ReturnCode = packets.ErrRefusedNotAuthorised
		if err := connack.Write(conn); err != nil {
			return fmt.Errorf("send connack error:%v,clientID:%v,conn:%v", err, msg.ClientIdentifier, conn)
		}
		return fmt.Errorf("connect packet CheckConnectAuth failed with connack.ReturnCode%v", connack.ReturnCode)
	}

	if err := connack.Write(conn); err != nil {
		return fmt.Errorf("send connack error:%v,clientID:%v,conn:%v", err, msg.ClientIdentifier, conn)
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
		password:  msg.Password,
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

	if err := b.getSession(c, msg, connack); err != nil {
		return fmt.Errorf("get session error:%v,clientID:%v,conn:%v", err, msg.ClientIdentifier, conn)
	}

	cid := c.info.clientID

	var exists bool
	var old interface{}

	switch typ {
	case CLIENT:
		old, exists = b.clients.Load(cid)
		if exists {
			if ol, ok := old.(*client); ok {
				log.Warn("client exists, close old client", getAdditionalLogFields(ol.info.clientID, ol.conn)...)
				ol.Close()
			}
		}
		b.clients.Store(cid, c)

		var pubPack = PubPacket{}
		if willmsg != nil {
			pubPack.TopicName = info.willMsg.TopicName
			pubPack.Payload = info.willMsg.Payload
		}

		pubInfo := Info{
			ClientID:  info.clientID,
			Username:  info.username,
			Password:  info.password,
			Keepalive: info.keepalive,
			WillMsg:   pubPack,
		}

		b.OnlineOfflineNotification(pubInfo, true, c.lastMsgTime)
		{
			b.Publish(&bridge.Elements{
				ClientID:  msg.ClientIdentifier,
				Username:  msg.Username,
				Action:    bridge.Connect,
				Timestamp: time.Now().Unix(),
			})
		}
	case ROUTER:
		old, exists = b.routes.Load(cid)
		if exists {
			if ol, ok := old.(*client); ok {
				log.Warn("router exists, close old router", getAdditionalLogFields(ol.info.clientID, ol.conn)...)
				ol.Close()
			}
		}
		b.routes.Store(cid, c)
	}

	c.readLoop()
	return nil
}

func (b *Broker) ConnectToDiscovery() {
	var conn net.Conn
	var err error
	var tempDelay time.Duration = 0
	for {
		conn, err = net.Dial("tcp", b.config.Router)
		if err != nil {
			log.Error("Error trying to connect to route", zap.Error(err))
			log.Debug("Connect to route timeout, retry...")

			if tempDelay == 0 {
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
	log.Debug("connect to router success", zap.String("Router", b.config.Router))

	cid := b.id
	info := info{
		clientID:  cid,
		keepalive: 60,
	}

	c := &client{
		typ:    CLUSTER,
		broker: b,
		conn:   conn,
		info:   info,
	}

	c.init()

	c.SendConnect()
	c.SendInfo()

	go c.readLoop()
	go c.StartPing()
}

func (b *Broker) processClusterInfo() {
	for {
		msg, ok := <-b.clusterPool
		if !ok {
			log.Error("read message from cluster channel error")
			return
		}
		ProcessMessage(msg)
	}

}

func (b *Broker) connectRouter(id, addr string) {
	var conn net.Conn
	var err error
	var timeDelay time.Duration = 0
	retryTimes := 0
	max := 32 * time.Second
	for {

		if !b.checkNodeExist(id, addr) {
			return
		}

		conn, err = net.Dial("tcp", addr)
		if err != nil {
			log.Error("Error trying to connect to route", zap.Error(err))

			if retryTimes > 50 {
				return
			}

			log.Debug("Connect to route timeout, retry...")

			if timeDelay == 0 {
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
		broker: b,
		typ:    REMOTE,
		conn:   conn,
		route:  route,
		info:   info,
	}
	c.init()
	b.remotes.Store(cid, c)

	c.SendConnect()

	go c.readLoop()
	go c.StartPing()

}

func (b *Broker) checkNodeExist(id, url string) bool {
	if id == b.id {
		return false
	}

	for k, v := range b.nodes {
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

func (b *Broker) CheckRemoteExist(remoteID, url string) bool {
	exists := false
	b.remotes.Range(func(key, value interface{}) bool {
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

func (b *Broker) SendLocalSubsToRouter(c *client) {
	subInfo := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	b.clients.Range(func(key, value interface{}) bool {
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
		if err := c.WriterPacket(subInfo); err != nil {
			log.Error("Send localsubs To Router error", zap.Error(err))
		}
	}
}

func (b *Broker) BroadcastInfoMessage(remoteID string, msg *packets.PublishPacket) {
	b.routes.Range(func(key, value interface{}) bool {
		if r, ok := value.(*client); ok {
			if r.route.remoteID == remoteID {
				return true
			}
			r.WriterPacket(msg)
		}
		return true
	})
}

func (b *Broker) BroadcastSubOrUnsubMessage(packet packets.ControlPacket) {

	b.routes.Range(func(key, value interface{}) bool {
		if r, ok := value.(*client); ok {
			r.WriterPacket(packet)
		}
		return true
	})
}

func (b *Broker) removeClient(c *client) {
	clientId := c.info.clientID
	typ := c.typ
	switch typ {
	case CLIENT:
		b.clients.Delete(clientId)
	case ROUTER:
		b.routes.Delete(clientId)
	case REMOTE:
		b.remotes.Delete(clientId)
	}
}

func (b *Broker) PublishMessage(packet *packets.PublishPacket) {
	var subs []interface{}
	var qoss []byte
	b.mu.Lock()
	err := b.topicsMgr.Subscribers([]byte(packet.TopicName), packet.Qos, &subs, &qoss)
	b.mu.Unlock()
	if err != nil {
		log.Error("search sub client error", zap.Error(err))
		return
	}

	for _, sub := range subs {
		s, ok := sub.(*subscription)
		if ok {
			if err := s.client.WriterPacket(packet); err != nil {
				log.Error("write message error", zap.Error(err))
			}
		}
	}
}

func (b *Broker) PublishMessageByClientId(packet *packets.PublishPacket, clientId string) error {
	cli, loaded := b.clients.LoadAndDelete(clientId)
	if !loaded {
		return fmt.Errorf("clientId %s not connected", clientId)
	}
	conn, success := cli.(*client)
	if !success {
		return fmt.Errorf("clientId %s loaded fail", clientId)
	}
	return conn.WriterPacket(packet)
}

func (b *Broker) BroadcastUnSubscribe(topicsToUnSubscribeFrom []string) {
	if len(topicsToUnSubscribeFrom) == 0 {
		return
	}

	unsub := packets.NewControlPacket(packets.Unsubscribe).(*packets.UnsubscribePacket)
	unsub.Topics = append(unsub.Topics, topicsToUnSubscribeFrom...)
	b.BroadcastSubOrUnsubMessage(unsub)
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
		//This is a TERRIBLE situation, falling back to legacy format to not break API Contract
		packet.Payload = []byte(fmt.Sprintf(`{"clientID":"%s","online":%v,"timestamp":"%s"}`, info.ClientID, online, time.Now().UTC().Format(time.RFC3339)))
	} else {
		packet.Payload = b
	}

	b.PublishMessage(packet)
}
