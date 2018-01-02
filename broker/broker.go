package broker

import (
	"crypto/tls"
	"hmq/lib/acl"
	"net"
	"net/http"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/shirou/gopsutil/mem"

	"golang.org/x/net/websocket"

	log "github.com/cihub/seelog"
)

type Broker struct {
	id        string
	cid       uint64
	config    *Config
	tlsConfig *tls.Config
	AclConfig *acl.ACLConfig
	clients   sync.Map
	routes    sync.Map
	remotes   sync.Map
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
		queues: make(map[string]int),
	}
	if b.config.TlsPort != "" {
		tlsconfig, err := NewTLSConfig(b.config.TlsInfo)
		if err != nil {
			log.Error("new tlsConfig error: ", err)
			return nil, err
		}
		b.tlsConfig = tlsconfig
	}
	if b.config.Acl {
		aclconfig, err := acl.AclConfigLoad(b.config.AclConf)
		if err != nil {
			log.Error("Load acl conf error: ", err)
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
	if len(b.config.Cluster.Routes) > 0 {
		b.ConnectToRouters()
	}

	//system montior
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
	log.Info("Start Webscoker Listening on ", hp, path)
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
		log.Info("Start TLS Listening client on ", hp)
	} else {
		hp := b.config.Host + ":" + b.config.Port
		l, err = net.Listen("tcp", hp)
		log.Info("Start Listening client on ", hp)
	}
	if err != nil {
		log.Error("Error listening on ", err)
		return
	}
	tmpDelay := 10 * ACCEPT_MIN_SLEEP
	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Error("Temporary Client Accept Error(%v), sleeping %dms",
					ne, tmpDelay/time.Millisecond)
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else {
				log.Error("Accept error: %v", err)
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
		log.Error("TLS handshake error, ", err)
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
	log.Info("Start Listening cluster on ", hp)

	l, e := net.Listen("tcp", hp)
	if e != nil {
		log.Error("Error listening on ", e)
		return
	}

	var idx uint64 = 0
	tmpDelay := 10 * ACCEPT_MIN_SLEEP
	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Error("Temporary Client Accept Error(%v), sleeping %dms",
					ne, tmpDelay/time.Millisecond)
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else {
				log.Error("Accept error: %v", err)
			}
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP

		go b.handleConnection(ROUTER, conn, idx)
		if idx == 1 {
			idx = 0
		} else {
			idx = idx + 1
		}

	}
}

func (b *Broker) handleConnection(typ int, conn net.Conn, idx uint64) {
	//process connect packet
	packet, err := packets.ReadPacket(conn)
	if err != nil {
		log.Error("read connect packet error: ", err)
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
		log.Error("send connack error, ", err, " clientID = ", msg.ClientIdentifier)
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
			log.Warn("client exist, close old...", " clientID = ", c.info.clientID)
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

func (b *Broker) ConnectToRouters() {
	for i := 0; i < len(b.config.Cluster.Routes); i++ {
		url := b.config.Cluster.Routes[i]
		go b.connectRouter(url, "")
	}
}

func (b *Broker) connectRouter(url, remoteID string) {
	var conn net.Conn
	var err error
	for {
		conn, err = net.Dial("tcp", url)
		if err != nil {
			log.Error("Error trying to connect to route: ", err)
			select {
			case <-time.After(DEFAULT_ROUTE_CONNECT):
				log.Debug("Connect to route timeout ,retry...")
				continue
			}
		}
	}
	route := &route{
		remoteID:  remoteID,
		remoteUrl: url,
	}
	cid := GenUniqueId()
	info := info{
		clientID: cid,
	}
	c := &client{
		typ:   REMOTE,
		conn:  conn,
		route: route,
		info:  info,
	}
	c.init()
	b.remotes.Store(cid, c)
	c.SendConnect()
	c.SendInfo()
	c.StartPing()
}

func (b *Broker) CheckRemoteExist(remoteID, url string) bool {
	exist := false
	b.remotes.Range(func(key, value interface{}) bool {
		v, ok := value.(*client)
		if ok {
			if v.route.remoteUrl == url {
				// if v.route.remoteID == "" || v.route.remoteID != remoteID {
				v.route.remoteID = remoteID
				// }
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
	err := c.WriterPacket(subInfo)
	if err != nil {
		log.Error("Send localsubs To Router error :", err)
	}
}

func (b *Broker) BroadcastInfoMessage(remoteID string, msg *packets.PublishPacket) {
	b.remotes.Range(func(key, value interface{}) bool {
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
	b.remotes.Range(func(key, value interface{}) bool {
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
	// log.Info("psubs num: ", len(r.psubs))
	if len(r.psubs) == 0 {
		return
	}

	for _, sub := range r.psubs {
		if sub != nil {
			err := sub.client.WriterPacket(packet)
			if err != nil {
				log.Error("process message for psub error,  ", err)
			}
		}
	}
}

func (b *Broker) BroadcastUnSubscribe(subs map[string]*subscription) {

	ubsub := packets.NewControlPacket(packets.Unsubscribe).(*packets.UnsubscribePacket)
	for topic, _ := range subs {
		// topic := sub.topic
		// if sub.queue {
		// 	topic = "$queue/" + sub.topic
		// }
		ubsub.Topics = append(ubsub.Topics, topic)
	}
	b.BroadcastSubOrUnsubMessage(ubsub)

}
