package broker

import (
	"fhmq/lib/message"
	"net"
	"time"

	log "github.com/cihub/seelog"
)

type Broker struct {
	id      string
	config  *Config
	clients ClientMap
	routes  ClientMap
	sl      *Sublist
	rl      *RetainList
	queues  map[string]int
}

func NewBroker(config *Config) *Broker {
	return &Broker{
		config:  config,
		sl:      NewSublist(),
		rl:      NewRetainList(),
		queues:  make(map[string]int),
		clients: NewClientMap(),
		routes:  NewClientMap(),
	}
}

func (b *Broker) Start() {
	go b.StartListening(CLIENT)
	if b.config.Cluster.Port != "" {
		go b.StartListening(ROUTER)
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
	num := 0
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
		num += 1
		go b.handleConnection(typ, conn, num)
	}
}

func (b *Broker) handleConnection(typ int, conn net.Conn, idx int) {
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
		broker: b,
		conn:   conn,
		info:   info,
	}
	c.init()

	var exist bool
	var old *client
	cid := string(c.info.clientID)
	if typ == CLIENT {
		old, exist = b.clients.Update(cid, c)
	} else if typ == ROUTER {
		old, exist = b.routes.Update(cid, c)
	}
	if exist {
		log.Warn("client or routers exists, close old...")
		old.Close()
	}
	c.readLoop(idx)
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
		// s.createRemote(conn, route)
		return
	}
}
