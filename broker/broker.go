/* Copyright (c) 2018, joy.zhou <chowyu08@gmail.com>
 */
package broker

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	_ "net/http/pprof"
	"runtime/debug"
	"sync"
	"time"

	"github.com/tidwall/gjson"

	"github.com/fhmq/hmq/plugins/auth"
	"github.com/fhmq/hmq/plugins/bridge"

	"github.com/fhmq/hmq/broker/lib/sessions"
	"github.com/fhmq/hmq/broker/lib/topics"
	pb "github.com/fhmq/hmq/grpc"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/pool"
	"github.com/shirou/gopsutil/mem"
	"go.uber.org/zap"
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
	nodes       map[string]gjson.Result
	clusterPool chan *Message
	topicsMgr   *topics.Manager
	sessionMgr  *sessions.Manager
	rpcClient   map[string]pb.HMQServiceClient
	Auth        auth.Auth
	BridgeMQ    bridge.BridgeMQ
}

func newMessagePool() []chan *Message {
	pool := make([]chan *Message, 0)
	for i := 0; i < MessagePoolNum; i++ {
		ch := make(chan *Message, MessagePoolMessageNum)
		pool = append(pool, ch)
	}
	return pool
}

func NewBroker(config *Config) (*Broker, error) {
	if config == nil {
		config = DefaultConfig
	}

	b := &Broker{
		id:          GenUniqueId(),
		config:      config,
		wpool:       pool.New(config.Worker),
		nodes:       make(map[string]gjson.Result),
		clusterPool: make(chan *Message),
		rpcClient:   make(map[string]pb.HMQServiceClient),
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

	b.Auth = auth.NewAuth(b.config.Plugin.Auth)
	b.BridgeMQ = bridge.NewBridgeMQ(b.config.Plugin.Bridge)

	return b, nil
}

func (b *Broker) SubmitWork(clientId string, msg *Message) {
	if b.wpool == nil {
		b.wpool = pool.New(b.config.Worker)
	}

	if msg.client.typ == ROUTER {
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

	go InitHTTPMoniter(b)

	//connet to router
	if b.config.Router != "" {
		go b.ConnectToDiscovery()
		go b.processClusterInfo()
		go b.initRPCService()
	}

	//listen for websocket
	if b.config.WsPort != "" {
		go b.StartWebsocketListening()
	}

	//listen client over tls
	if b.config.TlsPort != "" {
		go b.StartClientListening(true)
	}

	//listen clinet over tcp
	if b.config.Port != "" {
		go b.StartClientListening(false)
	}

	//system monitor
	go StateMonitor()

}

func StateMonitor() {
	v, _ := mem.VirtualMemory()
	timeSticker := time.NewTicker(time.Second * 60)
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
	log.Info("Start Websocket Listener on:", zap.String("hp", hp), zap.String("path", path))
	http.Handle(path, websocket.Handler(b.wsHandler))
	var err error
	if b.config.WsTLS {
		err = http.ListenAndServeTLS(hp, b.config.TlsInfo.CertFile, b.config.TlsInfo.KeyFile, nil)
	} else {
		err = http.ListenAndServe(hp, nil)
	}
	if err != nil {
		log.Error("ListenAndServe:" + err.Error())
		return
	}
}

func (b *Broker) wsHandler(ws *websocket.Conn) {
	// io.Copy(ws, ws)
	ws.PayloadType = websocket.BinaryFrame
	b.handleConnection(CLIENT, ws)
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
		go b.handleConnection(CLIENT, conn)

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

func (b *Broker) handleConnection(typ int, conn net.Conn) {
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

	log.Info("read connect from ", zap.String("clientID", msg.ClientIdentifier))

	connack := packets.NewControlPacket(packets.Connack).(*packets.ConnackPacket)
	connack.SessionPresent = msg.CleanSession
	connack.ReturnCode = msg.Validate()

	if connack.ReturnCode != packets.Accepted {
		err = connack.Write(conn)
		if err != nil {
			log.Error("send connack error, ", zap.Error(err), zap.String("clientID", msg.ClientIdentifier))
			return
		}
		return
	}

	if typ == CLIENT && !b.CheckConnectAuth(string(msg.ClientIdentifier), string(msg.Username), string(msg.Password)) {
		connack.ReturnCode = packets.ErrRefusedNotAuthorised
		err = connack.Write(conn)
		if err != nil {
			log.Error("send connack error, ", zap.Error(err), zap.String("clientID", msg.ClientIdentifier))
			return
		}
		return
	}

	err = connack.Write(conn)
	if err != nil {
		log.Error("send connack error, ", zap.Error(err), zap.String("clientID", msg.ClientIdentifier))
		return
	}

	if typ == CLIENT {
		b.Publish(&bridge.Elements{
			ClientID:  string(msg.ClientIdentifier),
			Username:  string(msg.Username),
			Action:    bridge.Connect,
			Timestamp: time.Now().Unix(),
		})
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

	err = b.getSession(c, msg, connack)
	if err != nil {
		log.Error("get session error: ", zap.String("clientID", c.info.clientID))
		return
	}

	cid := c.info.clientID

	var exist bool
	var old interface{}

	switch typ {
	case CLIENT:
		old, exist = b.clients.Load(cid)
		if exist {
			log.Warn("client exist, close old...", zap.String("clientID", c.info.clientID))
			ol, ok := old.(*client)
			if ok {
				ol.Close()
			}
		} else {
			go b.QueryConnect(cid)
		}
		b.clients.Store(cid, c)
		b.OnlineOfflineNotification(cid, true)
	}

	c.readLoop()
}

func (b *Broker) removeClient(c *client) {
	clientId := string(c.info.clientID)
	b.clients.Delete(clientId)
}

func (b *Broker) OnlineOfflineNotification(clientID string, online bool) {
	packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	packet.TopicName = "$SYS/broker/connection/clients/" + clientID
	packet.Qos = 0
	packet.Payload = []byte(fmt.Sprintf(`{"clientID":"%s","online":%v,"timestamp":"%s"}`, clientID, online, time.Now().UTC().Format(time.RFC3339)))

	b.PublishMessage(packet)
}

func (b *Broker) PublishDeliverdMessage(packet *packets.PublishPacket, share bool) {
	{
		//do retain
		if packet.Retain {
			if err := b.topicsMgr.Retain(packet); err != nil {
				log.Error("Error retaining message: ", zap.Error(err))
			}
		}
	}

	var subs []interface{}
	var qoss []byte
	err := b.topicsMgr.Subscribers([]byte(packet.TopicName), packet.Qos, &subs, &qoss)
	if err != nil {
		log.Error("search sub client error,  ", zap.Error(err))
		return
	}

	if len(subs) == 0 {
		return
	}

	var qsub []*subscription
	for _, sub := range subs {
		s, ok := sub.(*subscription)
		if ok {
			if s.share {
				qsub = append(qsub, s)
			} else {
				publish(s, packet)
			}
		}
	}

	if share {
		target := r.Intn(len(qsub))
		sub := qsub[target]
		publish(sub, packet)
	}
}

func (b *Broker) PublishMessage(packet *packets.PublishPacket) {
	{
		//do retain
		if packet.Retain {
			if err := b.topicsMgr.Retain(packet); err != nil {
				log.Error("Error retaining message: ", zap.Error(err))
			}
		}
	}

	var subs []interface{}
	var qoss []byte
	err := b.topicsMgr.Subscribers([]byte(packet.TopicName), packet.Qos, &subs, &qoss)
	if err != nil {
		log.Error("search sub client error,  ", zap.Error(err))
		return
	}

	if len(subs) == 0 {
		return
	}

	var qsub []*subscription
	for _, sub := range subs {
		s, ok := sub.(*subscription)
		if ok {
			if s.share {
				qsub = append(qsub, s)
			} else {
				publish(s, packet)
			}
		}
	}

	go b.ProcessRemote(packet, qsub)

}

func (b *Broker) ProcessRemote(packet *packets.PublishPacket, loaclShareSub []*subscription) {

	remoteSubInfo := b.QuerySubscribe(packet.TopicName, packet.Qos)

	//calc which node process share message
	shareRemoteID := ""
	{
		totalShare := len(loaclShareSub)
		for _, v := range remoteSubInfo {
			totalShare = totalShare + v.shareSubCount
		}

		target := r.Intn(totalShare)
		if target < len(loaclShareSub) {
			shareRemoteID = b.id
			//send local
			if shareRemoteID == b.id {
				sub := loaclShareSub[target]
				publish(sub, packet)
			}

		} else {
			target = target - len(loaclShareSub)
			for k, v := range remoteSubInfo {
				if target < v.shareSubCount {
					shareRemoteID = k
					return
				}
				target = target - v.shareSubCount
			}
		}
	}

	//send remote message
	for id, sub := range remoteSubInfo {
		rpcCli := b.rpcClient[id]
		if sub.subCount > 0 {
			rpcCli.DeliverMessage(context.Background(), &pb.DeliverMessageRequest{Topic: packet.TopicName, Payload: packet.Payload, Share: shareRemoteID == id})
		}
	}

}
