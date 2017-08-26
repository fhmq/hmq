package broker

import (
	"crypto/tls"
	"hmq/lib/acl"
	"hmq/lib/message"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"golang.org/x/net/websocket"

	log "github.com/cihub/seelog"
)

type Broker struct {
	id        string
	cid       uint64
	config    *Config
	tlsConfig *tls.Config
	AclConfig *acl.ACLConfig
	clients   cMap
	routes    cMap
	remotes   cMap
	sl        *Sublist
	rl        *RetainList
	queues    map[string]int
}

func NewBroker(config *Config) *Broker {
	b := &Broker{
		id:      GenUniqueId(),
		config:  config,
		sl:      NewSublist(),
		rl:      NewRetainList(),
		queues:  make(map[string]int),
		clients: NewClientMap(),
		routes:  NewClientMap(),
		remotes: NewClientMap(),
	}
	if b.config.TlsPort != "" {
		tlsconfig, err := NewTLSConfig(b.config.TlsInfo)
		if err != nil {
			log.Error("new tlsConfig error: ", err)
			return nil
		}
		b.tlsConfig = tlsconfig
	}
	if b.config.Acl {
		aclconfig, err := acl.AclConfigLoad(b.config.AclConf)
		if err != nil {
			log.Error("Load acl conf error: ", err)
			return nil
		}
		b.AclConfig = aclconfig
		b.StartAclWatcher()
	}
	return b
}

func (b *Broker) Start() {
	if b == nil {
		log.Error("broker is null")
		return
	}
	if b.config.Port != "" {
		go b.StartListening(CLIENT)
	}
	if b.config.Cluster.Port != "" {
		go b.StartListening(ROUTER)
	}
	if b.config.WsPort != "" {
		go b.StartWebsocketListening()
	}
	if b.config.TlsPort != "" {
		go b.StartTLSListening()
	}
}

func (b *Broker) StartWebsocketListening() {
	path := b.config.WsPath
	hp := ":" + b.config.WsPort
	log.Info("Start Webscoker Listening on ", hp, path)
	http.Handle(path, websocket.Handler(b.wsHandler))
	err := http.ListenAndServe(hp, nil)
	if err != nil {
		log.Error("ListenAndServe: " + err.Error())
		return
	}
}

func (b *Broker) wsHandler(ws *websocket.Conn) {
	atomic.AddUint64(&b.cid, 1)
	go b.handleConnection(CLIENT, ws, b.cid)
}

func (b *Broker) StartTLSListening() {
	hp := b.config.TlsHost + ":" + b.config.TlsPort
	log.Info("Start TLS Listening client on ", hp)

	l, e := tls.Listen("tcp", hp, b.tlsConfig)
	if e != nil {
		log.Error("Error listening on ", e)
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

func (b *Broker) StartListening(typ int) {
	var hp string
	if typ == CLIENT {
		hp = b.config.Host + ":" + b.config.Port
		log.Info("Start Listening client on ", hp)
	} else if typ == ROUTER {
		hp = b.config.Cluster.Host + ":" + b.config.Cluster.Port
		log.Info("Start Listening cluster on ", hp)
	}

	l, e := net.Listen("tcp", hp)
	if e != nil {
		log.Error("Error listening on ", e)
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
		go b.handleConnection(typ, conn, b.cid)
	}
}

func (b *Broker) handleConnection(typ int, conn net.Conn, idx uint64) {
	//process connect packet
	buf, err := ReadPacket(conn)
	if err != nil {
		log.Error("read connect packet error: ", err)
		return
	}
	connMsg, err := DecodeConnectMessage(buf)
	if err != nil {
		log.Error(err)
		return
	}

	connack := message.NewConnackMessage()
	connack.SetReturnCode(message.ConnectionAccepted)
	ack, _ := EncodeMessage(connack)
	err1 := WriteBuffer(conn, ack)
	if err1 != nil {
		log.Error("send connack error, ", err1)
		return
	}

	willmsg := message.NewPublishMessage()
	if connMsg.WillFlag() {
		willmsg.SetQoS(connMsg.WillQos())
		willmsg.SetPayload(connMsg.WillMessage())
		willmsg.SetRetain(connMsg.WillRetain())
		willmsg.SetTopic(connMsg.WillTopic())
		willmsg.SetDup(false)
	} else {
		willmsg = nil
	}
	info := info{
		clientID:  connMsg.ClientId(),
		username:  connMsg.Username(),
		password:  connMsg.Password(),
		keepalive: connMsg.KeepAlive(),
		willMsg:   willmsg,
	}

	c := &client{
		typ:    typ,
		broker: b,
		conn:   conn,
		info:   info,
	}
	c.init()

	var msgPool *MessagePool
	var exist bool
	var old *client
	cid := string(c.info.clientID)
	if typ == CLIENT {
		old, exist = b.clients.Update(cid, c)
		msgPool = MSGPool[idx%MessagePoolNum].GetPool()
	} else if typ == ROUTER {
		old, exist = b.routes.Update(cid, c)
		msgPool = MSGPool[MessagePoolNum].GetPool()
	}
	if exist {
		log.Warn("client or routers exists, close old...")
		old.Close()
	}
	c.readLoop(msgPool)
}

func (b *Broker) ConnectToRouters() {
	for i := 0; i < len(b.config.Cluster.Routes); i++ {
		url := b.config.Cluster.Routes[i]
		go b.connectRouter(url, "")
	}
}

func (b *Broker) connectRouter(url, remoteID string) {
	for {
		conn, err := net.Dial("tcp", url)
		if err != nil {
			log.Error("Error trying to connect to route: ", err)
			select {
			case <-time.After(DEFAULT_ROUTE_CONNECT):
				log.Debug("Connect to route timeout ,retry...")
				continue
			}
		}
		route := &route{
			remoteID:  remoteID,
			remoteUrl: url,
		}
		cid := GenUniqueId()
		info := info{
			clientID: []byte(cid),
		}
		c := &client{
			typ:   REMOTE,
			conn:  conn,
			route: route,
			info:  info,
		}
		b.remotes.Set(cid, c)
		c.SendConnect()
		c.SendInfo()
		// s.createRemote(conn, route)
		msgPool := MSGPool[(MessagePoolNum + 1)].GetPool()
		c.readLoop(msgPool)
	}
}

func (b *Broker) CheckRemoteExist(remoteID, url string) bool {
	exist := false
	remotes := b.remotes.Items()
	for _, v := range remotes {
		if v.route.remoteUrl == url {
			// if v.route.remoteID == "" || v.route.remoteID != remoteID {
			v.route.remoteID = remoteID
			// }
			exist = true
			break
		}
	}
	return exist
}

func (b *Broker) SendLocalSubsToRouter(c *client) {
	clients := b.clients.Items()
	subMsg := message.NewSubscribeMessage()
	for _, client := range clients {
		subs := client.subs
		for _, sub := range subs {
			subMsg.AddTopic(sub.topic, sub.qos)
		}
	}
	err := c.writeMessage(subMsg)
	if err != nil {
		log.Error("Send localsubs To Router error :", err)
	}
}

func (b *Broker) BroadcastInfoMessage(remoteID string, msg message.Message) {
	remotes := b.remotes.Items()
	for _, r := range remotes {
		if r.route.remoteID == remoteID {
			continue
		}
		r.writeMessage(msg)
	}
	// log.Info("BroadcastInfoMessage success ")
}

func (b *Broker) BroadcastSubOrUnsubMessage(buf []byte) {
	remotes := b.remotes.Items()
	for _, r := range remotes {
		r.writeBuffer(buf)
	}
	// log.Info("BroadcastSubscribeMessage remotes: ", s.remotes)
}

func (b *Broker) removeClient(c *client) {
	clientId := string(c.info.clientID)
	typ := c.typ
	switch typ {
	case CLIENT:
		b.clients.Remove(clientId)
	case ROUTER:
		b.routes.Remove(clientId)
	case REMOTE:
		b.remotes.Remove(clientId)
	}
	// log.Info("delete client ,", clientId)
}

func (b *Broker) ProcessPublishMessage(msg *message.PublishMessage) {
	if b == nil {
		return
	}
	topic := string(msg.Topic())

	r := b.sl.Match(topic)
	// log.Info("psubs num: ", len(r.psubs))
	if len(r.qsubs) == 0 && len(r.psubs) == 0 {
		return
	}

	for _, sub := range r.psubs {
		if sub != nil {
			err := sub.client.writeMessage(msg)
			if err != nil {
				log.Error("process message for psub error,  ", err)
			}
		}
	}

	for i, sub := range r.qsubs {
		// s.qmu.Lock()
		if cnt, exist := b.queues[string(sub.topic)]; exist && i == cnt {
			if sub != nil {
				err := sub.client.writeMessage(msg)
				if err != nil {
					log.Error("process will message for qsub error,  ", err)
				}
			}
			b.queues[topic] = (b.queues[topic] + 1) % len(r.qsubs)
			break
		}
		// s.qmu.Unlock()
	}
}
