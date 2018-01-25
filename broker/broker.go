/* Copyright (c) 2018, joy.zhou <chowyu08@gmail.com>
 */
package broker

import (
	"crypto/tls"
	"net"
	"net/http"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fhmq/hmq/lib/acl"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/shirou/gopsutil/mem"
	"go.uber.org/zap"

	"golang.org/x/net/websocket"

	"github.com/fhmq/hmq/logger"
)

var (
	log = logger.Get().Named("Broker")
)

type Broker struct {
	id        string
	cid       uint64
	mu        sync.Mutex
	config    *Config
	tlsConfig *tls.Config
	AclConfig *acl.ACLConfig
	clients   sync.Map
	routes    sync.Map
	remotes   sync.Map
	nodes     map[string]interface{}
	sl        *Sublist
	rl        *RetainList
	queues    map[string]int
}

func NewBroker(config *Config) (*Broker, error) {
	b := &Broker{
		id:     GenUniqueId(),
		config: config,
		sl:     NewSublist(),
		rl:     NewRetainList(),
		nodes:  make(map[string]interface{}),
		queues: make(map[string]int),
	}
	if b.config.TlsPort != "" {
		tlsconfig, err := NewTLSConfig(b.config.TlsInfo)
		if err != nil {
			log.Error("new tlsConfig error", zap.Error(err))
			return nil, err
		}
		b.tlsConfig = tlsconfig
	}
	if b.config.Acl {
		aclconfig, err := acl.AclConfigLoad(b.config.AclConf)
		if err != nil {
			log.Error("Load acl conf error", zap.Error(err))
			return nil, err
		}
		b.AclConfig = aclconfig
		b.StartAclWatcher()
	}
	return b, nil
}

func (b *Broker) Start() {
	if b == nil {
		log.Error("broker is null")
		return
	}
	StartDispatcher()

	//listen clinet over tcp
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
		b.ConnectToDiscovery()
	}

	//system monitor
	go StateMonitor()

}

func StateMonitor() {
	v, _ := mem.VirtualMemory()
	timeSticker := time.NewTicker(time.Second * 30)
	for {
		select {
		case <-timeSticker.C:
			if v.UsedPercent > 75 {
				debug.FreeOSMemory()
			}
		}
	}
}

func (b *Broker) StartWebsocketListening() {
	path := b.config.WsPath
	hp := ":" + b.config.WsPort
	log.Info("Start Websocket Listening on ", zap.String("hp", hp), zap.String("path", path))
	http.Handle(path, websocket.Handler(b.wsHandler))
	var err error
	if b.config.WsTLS {
		err = http.ListenAndServeTLS(hp, b.config.TlsInfo.CertFile, b.config.TlsInfo.KeyFile, nil)
	} else {
		err = http.ListenAndServe(hp, nil)
	}
	if err != nil {
		log.Error("ListenAndServe: " + err.Error())
		return
	}
}

func (b *Broker) wsHandler(ws *websocket.Conn) {
	// io.Copy(ws, ws)
	atomic.AddUint64(&b.cid, 1)
	ws.PayloadType = websocket.BinaryFrame
	b.handleConnection(CLIENT, ws, b.cid)
}

func (b *Broker) StartClientListening(Tls bool) {
	var hp string
	var err error
	var l net.Listener
	if Tls {
		hp = b.config.TlsHost + ":" + b.config.TlsPort
		l, err = tls.Listen("tcp", hp, b.tlsConfig)
		log.Info("Start TLS Listening client on ", zap.String("hp", hp))
	} else {
		hp := b.config.Host + ":" + b.config.Port
		l, err = net.Listen("tcp", hp)
		log.Info("Start Listening client on ", zap.String("hp", hp))
	}
	if err != nil {
		log.Error("Error listening on ", zap.Error(err))
		return
	}
	tmpDelay := 10 * ACCEPT_MIN_SLEEP
	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Error("Temporary Client Accept Error(%v), sleeping %dms",
					zap.Error(ne), zap.Duration("sleeping", tmpDelay/time.Millisecond))
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else {
				log.Error("Accept error: %v", zap.Error(err))
			}
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP
		atomic.AddUint64(&b.cid, 1)
		go b.handleConnection(CLIENT, conn, b.cid)

	}
}

func (b *Broker) Handshake(conn net.Conn) bool {

	nc := tls.Server(conn, b.tlsConfig)
	time.AfterFunc(DEFAULT_TLS_TIMEOUT, func() { TlsTimeout(nc) })
	nc.SetReadDeadline(time.Now().Add(DEFAULT_TLS_TIMEOUT))

	// Force handshake
	if err := nc.Handshake(); err != nil {
		log.Error("TLS handshake error, ", zap.Error(err))
		return false
	}
	nc.SetReadDeadline(time.Time{})
	return true

}

func TlsTimeout(conn *tls.Conn) {
	nc := conn
	// Check if already closed
	if nc == nil {
		return
	}
	cs := nc.ConnectionState()
	if !cs.HandshakeComplete {
		log.Error("TLS handshake timeout")
		nc.Close()
	}
}

func (b *Broker) StartClusterListening() {
	var hp string = b.config.Cluster.Host + ":" + b.config.Cluster.Port
	log.Info("Start Listening cluster on ", zap.String("hp", hp))

	l, e := net.Listen("tcp", hp)
	if e != nil {
		log.Error("Error listening on ", zap.Error(e))
		return
	}

	var idx uint64 = 0
	tmpDelay := 10 * ACCEPT_MIN_SLEEP
	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Error("Temporary Client Accept Error(%v), sleeping %dms",
					zap.Error(ne), zap.Duration("sleeping", tmpDelay/time.Millisecond))
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else {
				log.Error("Accept error: %v", zap.Error(err))
			}
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP

		go b.handleConnection(ROUTER, conn, idx)
	}
}

func (b *Broker) handleConnection(typ int, conn net.Conn, idx uint64) {
	//process connect packet
	packet, err := packets.ReadPacket(conn)
	if err != nil {
		log.Error("read connect packet error: ", zap.Error(err))
		return
	}
	if packet == nil {
		log.Error("received nil packet")
		return
	}
	msg, ok := packet.(*packets.ConnectPacket)
	if !ok {
		log.Error("received msg that was not Connect")
		return
	}
	connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	connack.ReturnCode = packets.Accepted
	connack.SessionPresent = msg.CleanSession
	err = connack.Write(conn)
	if err != nil {
		log.Error("send connack error, ", zap.Error(err), zap.String("clientID", msg.ClientIdentifier))
		return
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

	cid := c.info.clientID

	var msgPool *MessagePool
	var exist bool
	var old interface{}

	switch typ {
	case CLIENT:
		msgPool = MSGPool[idx%MessagePoolNum].GetPool()
		c.mp = msgPool
		old, exist = b.clients.Load(cid)
		if exist {
			log.Warn("client exist, close old...", zap.String("clientID", c.info.clientID))
			ol, ok := old.(*client)
			if ok {
				msg := &Message{client: c, packet: DisconnectdPacket}
				ol.mp.queue <- msg
			}
		}
		b.clients.Store(cid, c)
	case ROUTER:
		msgPool = MSGPool[(MessagePoolNum + idx)].GetPool()
		c.mp = msgPool
		old, exist = b.routes.Load(cid)
		if exist {
			log.Warn("router exist, close old...")
			ol, ok := old.(*client)
			if ok {
				msg := &Message{client: c, packet: DisconnectdPacket}
				ol.mp.queue <- msg
			}
		}
		b.routes.Store(cid, c)
	}

	c.readLoop()
}

func (b *Broker) ConnectToDiscovery() {
	var conn net.Conn
	var err error
	var tempDelay time.Duration = 0
	for {
		conn, err = net.Dial("tcp", b.config.Router)
		if err != nil {
			log.Error("Error trying to connect to route: ", zap.Error(err))
			log.Debug("Connect to route timeout ,retry...")

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
	log.Debug("connect to router success :", zap.String("Router", b.config.Router))

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

	c.mp = &MSGPool[(MessagePoolNum + 2)]
	go c.readLoop()
	go c.StartPing()
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
			log.Error("Error trying to connect to route: ", zap.Error(err))

			if retryTimes > 50 {
				return
			}

			log.Debug("Connect to route timeout ,retry...")

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
		broker: b,
		typ:    REMOTE,
		conn:   conn,
		route:  route,
		info:   info,
	}
	c.init()
	b.remotes.Store(cid, c)

	c.mp = MSGPool[(MessagePoolNum + 1)].GetPool()

	c.SendConnect()
	// c.SendInfo()

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
	exist := false
	b.remotes.Range(func(key, value interface{}) bool {
		v, ok := value.(*client)
		if ok {
			if v.route.remoteUrl == url {
				v.route.remoteID = remoteID
				exist = true
				return false
			}
		}
		return true
	})
	return exist
}

func (b *Broker) SendLocalSubsToRouter(c *client) {
	subInfo := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
	b.clients.Range(func(key, value interface{}) bool {
		client, ok := value.(*client)
		if ok {
			subs := client.subs
			for _, sub := range subs {
				subInfo.Topics = append(subInfo.Topics, string(sub.topic))
				subInfo.Qoss = append(subInfo.Qoss, sub.qos)
			}
		}
		return true
	})
	if len(subInfo.Topics) > 0 {
		err := c.WriterPacket(subInfo)
		if err != nil {
			log.Error("Send localsubs To Router error :", zap.Error(err))
		}
	}
}

func (b *Broker) BroadcastInfoMessage(remoteID string, msg *packets.PublishPacket) {
	b.routes.Range(func(key, value interface{}) bool {
		r, ok := value.(*client)
		if ok {
			if r.route.remoteID == remoteID {
				return true
			}
			r.WriterPacket(msg)
		}
		return true

	})
	// log.Info("BroadcastInfoMessage success ")
}

func (b *Broker) BroadcastSubOrUnsubMessage(packet packets.ControlPacket) {

	b.routes.Range(func(key, value interface{}) bool {
		r, ok := value.(*client)
		if ok {
			r.WriterPacket(packet)
		}
		return true
	})
	// log.Info("BroadcastSubscribeMessage remotes: ", s.remotes)
}

func (b *Broker) removeClient(c *client) {
	clientId := string(c.info.clientID)
	typ := c.typ
	switch typ {
	case CLIENT:
		b.clients.Delete(clientId)
	case ROUTER:
		b.routes.Delete(clientId)
	case REMOTE:
		b.remotes.Delete(clientId)
	}
	// log.Info("delete client ,", clientId)
}

func (b *Broker) PublishMessage(packet *packets.PublishPacket) {
	topic := packet.TopicName
	r := b.sl.Match(topic)
	if len(r.psubs) == 0 {
		return
	}

	for _, sub := range r.psubs {
		if sub != nil {
			err := sub.client.WriterPacket(packet)
			if err != nil {
				log.Error("process message for psub error,  ", zap.Error(err))
			}
		}
	}
}

func (b *Broker) BroadcastUnSubscribe(subs map[string]*subscription) {

	unsub := packets.NewControlPacket(packets.Unsubscribe).(*packets.UnsubscribePacket)
	for topic, _ := range subs {
		unsub.Topics = append(unsub.Topics, topic)
	}

	if len(unsub.Topics) > 0 {
		b.BroadcastSubOrUnsubMessage(unsub)
	}
}
