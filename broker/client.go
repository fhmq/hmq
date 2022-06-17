package broker

import (
	"bytes"
	"context"
	"errors"
	"hmq/plugins/bridge"
	"io"
	"math/rand"
	"net"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/eapache/queue"

	"hmq/broker/packets"

	"hmq/broker/lib/sessions"
	"hmq/broker/lib/topics"

	"go.uber.org/zap"
	"golang.org/x/net/websocket"
)

const (
	// InfoTopic special pub topic for cluster info
	InfoTopic = "broker000100101info"

	// CLIENT is an end user.
	CLIENT = 0

	// ROUTER is another router in the cluster.
	ROUTER = 1

	// REMOTE is the router connect to other cluster
	REMOTE = 2

	// CLUSTER is the CLUSTER itself
	CLUSTER = 3
)

const (
	_GroupTopicRegexp = `^\$share/([0-9a-zA-Z_-]+)/(.*)$`
)

// Possible values for client status field
const (
	// Connected means that client is currently connected
	Connected = 1

	// Disconnected means that client is currently disconnected
	Disconnected = 2
)

const (
	awaitRelTimeout int64 = 20
	retryInterval   int64 = 20
)

var (
	groupCompile = regexp.MustCompile(_GroupTopicRegexp)
)

type client struct {
	category       int
	mu             sync.Mutex
	broker         *Broker
	conn           net.Conn
	info           info
	route          route
	status         int
	ctx            context.Context
	cancelFunc     context.CancelFunc
	session        *sessions.Session
	subMap         map[string]*subscription
	subMapMu       sync.RWMutex
	topicsMgr      *topics.Manager
	subs           []interface{}
	qoss           []byte
	rmsgs          []*packets.PublishPacket
	routeSubMap    map[string]uint64
	routeSubMapMu  sync.Mutex
	awaitingRel    map[uint16]int64
	awaitingRelMu  sync.RWMutex
	maxAwaitingRel int
	inflight       map[uint16]*inflightElem
	inflightMu     sync.RWMutex
	mqueue         *queue.Queue
	retryTimer     *time.Timer
	retryTimerLock sync.Mutex
}

type InflightStatus uint8

const (
	Publish InflightStatus = 0
	PubRel  InflightStatus = 1
)

type inflightElem struct {
	status    InflightStatus
	packet    *packets.PublishPacket
	timestamp int64
}
type subscription struct {
	client    *client
	topic     string
	qos       byte
	share     bool
	groupName string
}

type info struct {
	clientID  string
	username  string
	password  []byte
	keepalive uint16
	willMsg   *packets.PublishPacket
	localIP   string
	remoteIP  string
}

type route struct {
	remoteID  string
	remoteUrl string
}

var (
	DisconnectedPacket = packets.NewControlPacket(packets.Disconnect).(*packets.DisconnectPacket)
	r                  = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// init function initializes a new client
func (c *client) init() {
	c.status = Connected
	c.info.localIP, _, _ = net.SplitHostPort(c.conn.LocalAddr().String())
	remoteAddr := c.conn.RemoteAddr()
	remoteNetwork := remoteAddr.Network()
	c.info.remoteIP = ""
	if remoteNetwork != "websocket" {
		c.info.remoteIP, _, _ = net.SplitHostPort(remoteAddr.String())
	} else {
		ws := c.conn.(*websocket.Conn)
		c.info.remoteIP, _, _ = net.SplitHostPort(ws.Request().RemoteAddr)
	}
	c.ctx, c.cancelFunc = context.WithCancel(context.Background())
	c.subMap = make(map[string]*subscription)
	c.topicsMgr = c.broker.topicsMgr
	c.routeSubMap = make(map[string]uint64)
	c.awaitingRel = make(map[uint16]int64)
	c.inflight = make(map[uint16]*inflightElem)
	c.mqueue = queue.New()
}

func (c *client) readLoop() {
	netConn := c.conn
	broker := c.broker

	// TODO: improve error handling
	if netConn == nil || broker == nil {
		return
	}

	// Configure both keep alive and timeout
	keepAlive := time.Second * time.Duration(c.info.keepalive)
	timeOut := keepAlive + (keepAlive / 2)

	for {

		var readFromClient = func(ctx context.Context) {
			//add read timeout
			if keepAlive > 0 {
				if err := netConn.SetReadDeadline(time.Now().Add(timeOut)); err != nil {
					log.Error("set read timeout error: ", zap.Error(err), zap.String("ClientID", c.info.clientID))
					msg := &Message{
						client: c,
						packet: DisconnectedPacket,
					}
					broker.SubmitWork(c.info.clientID, msg)
					return
				}
			}

			packet, err := packets.ReadPacket(netConn)
			if err != nil {
				if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) {
					log.Error("client closed its connection", zap.String("ClientID", c.info.clientID))
				} else {
					log.Error("read packet error: ", zap.Error(err), zap.String("ClientID", c.info.clientID))
				}

				msg := &Message{
					client: c,
					packet: DisconnectedPacket,
				}

				broker.SubmitWork(c.info.clientID, msg)
				return
			}

			// if packet is disconnect from client, then need to break the read packet loop and clear will msg.
			if discPacket, isDisconnect := packet.(*packets.DisconnectPacket); isDisconnect {
				/*
				 * The Server MUST validate that reserved bits are set to zero and
				 * disconnect the Client if they are not zero
				 */
				if !discPacket.IsValid() {
					c.info.willMsg = nil
					c.Close()
					log.Error("client forced to disconnect due to malformed packet", zap.String("ClientID", c.info.clientID))
					return
				}

				c.info.willMsg = nil
				c.cancelFunc()
			}

			msg := CreateNewMessage(c, packet)

			// Send message to thread pool
			broker.SubmitWork(c.info.clientID, msg)
		}

		readFromClient(c.ctx)

		select {
		case <-c.ctx.Done():
			return
		}
	}

}

// extractPacketFields function reads a control packet and extracts only the fields
// that needs to pass on UTF-8 validation
func extractPacketFields(msgPacket packets.ControlPacket) []string {
	var fields []string

	// Get packet type
	switch msgPacket.(type) {
	case *packets.ConnackPacket:
	case *packets.ConnectPacket:
		packet := msgPacket.(*packets.ConnectPacket)
		fields = packet.GetStringFields()
	case *packets.PublishPacket:
		packet := msgPacket.(*packets.PublishPacket)
		fields = append(fields, packet.TopicName)
		break

	case *packets.SubscribePacket:
	case *packets.SubackPacket:
	case *packets.UnsubscribePacket:
		packet := msgPacket.(*packets.UnsubscribePacket)
		fields = append(fields, packet.Topics...)
		break
	}

	return fields
}

// validatePacketFields function checks if any of control packets fields has ill-formed
// UTF-8 string
func validatePacketFields(msgPacket packets.ControlPacket) (validFields bool) {

	// Extract just fields that needs validation
	fields := extractPacketFields(msgPacket)

	for _, field := range fields {

		// Perform the basic UTF-8 validation
		if !utf8.ValidString(field) {
			validFields = false
			return
		}

		// A UTF-8 encoded string MUST NOT include an encoding of the null
		// character U+0000
		// If a receiver (Server or Client) receives a Control Packet containing U+0000
		// it MUST close the Network Connection
		// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.pdf page 14
		if bytes.ContainsAny([]byte(field), "\u0000") {
			validFields = false
			return
		}
	}

	// All fields have been validated successfully
	validFields = true

	return
}

// ProcessClientMessage  handles a packet sent from client to server
func ProcessClientMessage(msg *Message) {
	c := msg.client
	clientPacket := msg.packet

	if clientPacket == nil {
		log.Debug("Client packet is null", zap.String("ClientID", c.info.clientID))
		return
	}

	if c.category == CLIENT {
		log.Debug("Recv message:", zap.String("message type", reflect.TypeOf(msg.packet).String()[9:]), zap.String("ClientID", c.info.clientID))
	}

	switch clientPacket.(type) {
	case *packets.ConnackPacket:
	case *packets.ConnectPacket:
	case *packets.PublishPacket:
		packet := clientPacket.(*packets.PublishPacket)
		c.ProcessPublish(packet)
	case *packets.PubackPacket:
		packet := clientPacket.(*packets.PubackPacket)
		c.processPubAck(packet)
	case *packets.PubrecPacket:
		packet := clientPacket.(*packets.PubrecPacket)
		c.inflightMu.RLock()
		ielem, found := c.inflight[packet.MessageID]
		c.inflightMu.RUnlock()
		if found {
			if ielem.status == Publish {
				ielem.status = PubRel
				ielem.timestamp = time.Now().Unix()
			} else {
				log.Error("Duplicated PUBREC")
			}
		} else {
			log.Error("The PUBREC PacketId is not found.", zap.Uint16("MessageID", packet.MessageID))
		}

		pubrel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
		pubrel.MessageID = packet.MessageID
		if err := c.WriterPacket(pubrel); err != nil {
			log.Error("send pubrel error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			return
		}
	case *packets.PubrelPacket:
		packet := clientPacket.(*packets.PubrelPacket)
		_ = c.pubRel(packet.MessageID)
		pubcomp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
		pubcomp.MessageID = packet.MessageID
		if err := c.WriterPacket(pubcomp); err != nil {
			log.Error("send pubcomp error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			return
		}
	case *packets.PubcompPacket:
		packet := clientPacket.(*packets.PubcompPacket)
		c.inflightMu.Lock()
		delete(c.inflight, packet.MessageID)
		c.inflightMu.Unlock()
	case *packets.SubscribePacket:
		packet := clientPacket.(*packets.SubscribePacket)
		c.ProcessSubscribe(packet)
	case *packets.SubackPacket:
	case *packets.UnsubscribePacket:
		packet := clientPacket.(*packets.UnsubscribePacket)
		c.ProcessUnSubscribe(packet)
	case *packets.UnsubackPacket:
	case *packets.PingreqPacket:
		c.ProcessPing()
	case *packets.PingrespPacket:
	case *packets.DisconnectPacket:
		c.Close()

	default:
		log.Info("Recv Unknow message.......", zap.String("ClientID", c.info.clientID))
	}
}

func (c *client) ProcessPublish(packet *packets.PublishPacket) {
	switch c.category {
	case CLIENT:
		c.processClientPublish(packet)
	case ROUTER:
		c.processRouterPublish(packet)
	case CLUSTER:
		c.processRemotePublish(packet)
	}

}

func (c *client) processRemotePublish(packet *packets.PublishPacket) {
	if c.status == Disconnected {
		return
	}

	topic := packet.TopicName
	if topic == InfoTopic {
		c.ProcessInfo(packet)
		return
	}

}

func (c *client) processRouterPublish(packet *packets.PublishPacket) {
	if c.status == Disconnected {
		return
	}

	switch packet.Qos {
	case QosAtMostOnce:
		c.ProcessPublishMessage(packet)
	case QosAtLeastOnce:
		puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		puback.MessageID = packet.MessageID
		if err := c.WriterPacket(puback); err != nil {
			log.Error("send puback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			return
		}
		c.ProcessPublishMessage(packet)
	case QosExactlyOnce:
		return
	default:
		log.Error("publish with unknown qos", zap.String("ClientID", c.info.clientID))
		return
	}

}

func (c *client) processClientPublish(packet *packets.PublishPacket) {

	topic := packet.TopicName

	if !c.broker.CheckTopicAuth(PUB, c.info.clientID, c.info.username, c.info.remoteIP, topic) {
		log.Error("Pub Topics Auth failed, ", zap.String("topic", topic), zap.String("ClientID", c.info.clientID))
		return
	}

	//publish to bridge mq
	cost := c.broker.Publish(&bridge.Elements{
		ClientID:  c.info.clientID,
		Username:  c.info.username,
		Action:    bridge.Publish,
		Timestamp: time.Now().Unix(),
		Payload:   string(packet.Payload),
		Topic:     topic,
	})

	if cost {
		return
	}

	switch packet.Qos {
	case QosAtMostOnce:
		c.ProcessPublishMessage(packet)
	case QosAtLeastOnce:
		puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		puback.MessageID = packet.MessageID
		if err := c.WriterPacket(puback); err != nil {
			log.Error("send puback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			return
		}
		c.ProcessPublishMessage(packet)
	case QosExactlyOnce:
		if err := c.registerPublishPacketId(packet.MessageID); err != nil {
			return
		} else {
			pubrec := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
			pubrec.MessageID = packet.MessageID
			if err := c.WriterPacket(pubrec); err != nil {
				log.Error("send pubrec error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
				return
			}
			c.ProcessPublishMessage(packet)
		}

		return
	default:
		log.Error("publish with unknown qos", zap.String("ClientID", c.info.clientID))
		return
	}

}

func (c *client) processPubAck(packet *packets.PubackPacket) {
	c.inflightMu.Lock()
	defer c.inflightMu.Unlock()
	delete(c.inflight, packet.MessageID)
}

func (c *client) ProcessPublishMessage(packet *packets.PublishPacket) {

	broker := c.broker
	if broker == nil {
		return
	}

	clientCategory := c.category

	// Process retain flag
	if packet.Retain {
		if err := c.topicsMgr.Retain(packet); err != nil {
			log.Error("Error retaining message: ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		}
	}

	// Retrieve a list of all subscribers to a matching topic
	err := c.topicsMgr.Subscribers([]byte(packet.TopicName), packet.Qos, &c.subs, &c.qoss)
	if err != nil {
		log.Error("Error retrieving subscribers list: ", zap.String("ClientID", c.info.clientID))
		return
	}

	// Go back if there is not subscribers
	if len(c.subs) == 0 {
		return
	}

	var subscribersQoS []int

	for i, subscriptions := range c.subs {
		s, ok := subscriptions.(*subscription)
		if ok {
			if s.client.category == ROUTER && clientCategory != CLIENT {
				continue
			}

			if s.share {
				subscribersQoS = append(subscribersQoS, i)
			} else {
				publishToSubscribers(s, packet)
			}
		}
	}

	if len(subscribersQoS) > 0 {
		index := r.Intn(len(subscribersQoS))
		sub := c.subs[subscribersQoS[index]].(*subscription)
		publishToSubscribers(sub, packet)
	}

}

func (c *client) ProcessSubscribe(packet *packets.SubscribePacket) {
	switch c.category {
	case CLIENT:
		c.processClientSubscribe(packet)
	case ROUTER:
		fallthrough
	case REMOTE:
		c.processRouterSubscribe(packet)
	}
}

func (c *client) processClientSubscribe(packet *packets.SubscribePacket) {
	if c.status == Disconnected {
		return
	}

	b := c.broker
	if b == nil {
		return
	}

	subTopics := packet.Topics
	qoss := packet.Qoss

	suback := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	suback.MessageID = packet.MessageID
	var retcodes []byte

	for i, topic := range subTopics {
		t := topic
		//check topic auth for client
		if !b.CheckTopicAuth(SUB, c.info.clientID, c.info.username, c.info.remoteIP, topic) {
			log.Error("Sub topic Auth failed: ", zap.String("topic", topic), zap.String("ClientID", c.info.clientID))
			retcodes = append(retcodes, QosFailure)
			continue
		}

		b.Publish(&bridge.Elements{
			ClientID:  c.info.clientID,
			Username:  c.info.username,
			Action:    bridge.Subscribe,
			Timestamp: time.Now().Unix(),
			Topic:     topic,
		})

		groupName := ""
		share := false
		if strings.HasPrefix(topic, "$share/") {
			substr := groupCompile.FindStringSubmatch(topic)
			if len(substr) != 3 {
				retcodes = append(retcodes, QosFailure)
				continue
			}
			share = true
			groupName = substr[1]
			topic = substr[2]
		}

		c.subMapMu.Lock()
		if oldSub, exist := c.subMap[t]; exist {
			_ = c.topicsMgr.Unsubscribe([]byte(oldSub.topic), oldSub)
			delete(c.subMap, t)
		}
		c.subMapMu.Unlock()

		sub := &subscription{
			topic:     topic,
			qos:       qoss[i],
			client:    c,
			share:     share,
			groupName: groupName,
		}

		rqos, err := c.topicsMgr.Subscribe([]byte(topic), qoss[i], sub)
		if err != nil {
			log.Error("subscribe error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			retcodes = append(retcodes, QosFailure)
			continue
		}

		c.subMapMu.Lock()
		c.subMap[t] = sub
		c.subMapMu.Unlock()

		_ = c.session.AddTopic(t, qoss[i])
		retcodes = append(retcodes, rqos)
		_ = c.topicsMgr.Retained([]byte(topic), &c.rmsgs)
	}

	suback.ReturnCodes = retcodes

	err := c.WriterPacket(suback)
	if err != nil {
		log.Error("send suback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}

	//broadcast subscribe message
	go b.BroadcastSubOrUnsubMessage(packet)

	//process retain message
	for _, rm := range c.rmsgs {
		if err := c.WriterPacket(rm); err != nil {
			log.Error("Error publishing retained message:", zap.Any("err", err), zap.String("ClientID", c.info.clientID))
		} else {
			log.Info("process retain  message: ", zap.Any("packet", packet), zap.String("ClientID", c.info.clientID))
		}
	}
}

func (c *client) processRouterSubscribe(packet *packets.SubscribePacket) {
	if c.status == Disconnected {
		return
	}

	b := c.broker
	if b == nil {
		return
	}

	subTopics := packet.Topics
	qoss := packet.Qoss

	suback := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	suback.MessageID = packet.MessageID
	var retcodes []byte

	for i, topic := range subTopics {
		t := topic
		groupName := ""
		share := false
		if strings.HasPrefix(topic, "$share/") {
			substr := groupCompile.FindStringSubmatch(topic)
			if len(substr) != 3 {
				retcodes = append(retcodes, QosFailure)
				continue
			}
			share = true
			groupName = substr[1]
			topic = substr[2]
		}

		sub := &subscription{
			topic:     topic,
			qos:       qoss[i],
			client:    c,
			share:     share,
			groupName: groupName,
		}

		rqos, err := c.topicsMgr.Subscribe([]byte(topic), qoss[i], sub)
		if err != nil {
			log.Error("subscribe error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			retcodes = append(retcodes, QosFailure)
			continue
		}

		c.subMapMu.Lock()
		c.subMap[t] = sub
		c.subMapMu.Unlock()

		c.routeSubMapMu.Lock()
		addSubMap(c.routeSubMap, topic)
		c.routeSubMapMu.Unlock()
		retcodes = append(retcodes, rqos)
	}

	suback.ReturnCodes = retcodes

	err := c.WriterPacket(suback)
	if err != nil {
		log.Error("send suback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}
}

func (c *client) ProcessUnSubscribe(packet *packets.UnsubscribePacket) {
	switch c.category {
	case CLIENT:
		c.processClientUnSubscribe(packet)
	case ROUTER:
		c.processRouterUnSubscribe(packet)
	}
}

func (c *client) processRouterUnSubscribe(packet *packets.UnsubscribePacket) {
	if c.status == Disconnected {
		return
	}
	b := c.broker
	if b == nil {
		return
	}

	unSubTopics := packet.Topics

	for _, topic := range unSubTopics {
		c.subMapMu.Lock()
		if sub, exist := c.subMap[topic]; exist {
			c.routeSubMapMu.Lock()
			if retainNum := delSubMap(c.routeSubMap, topic); retainNum > 0 {
				c.routeSubMapMu.Unlock()
				c.subMapMu.Unlock()
				continue
			}
			c.routeSubMapMu.Unlock()

			_ = c.topicsMgr.Unsubscribe([]byte(sub.topic), sub)
			delete(c.subMap, topic)
		}
		c.subMapMu.Unlock()
	}

	unsuback := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	unsuback.MessageID = packet.MessageID

	err := c.WriterPacket(unsuback)
	if err != nil {
		log.Error("send unsuback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}
}

func (c *client) processClientUnSubscribe(packet *packets.UnsubscribePacket) {
	if c.status == Disconnected {
		return
	}
	b := c.broker
	if b == nil {
		return
	}

	unSubTopics := packet.Topics

	for _, topic := range unSubTopics {
		{
			//publish kafka

			b.Publish(&bridge.Elements{
				ClientID:  c.info.clientID,
				Username:  c.info.username,
				Action:    bridge.Unsubscribe,
				Timestamp: time.Now().Unix(),
				Topic:     topic,
			})

		}

		c.subMapMu.Lock()
		sub, exist := c.subMap[topic]
		if exist {
			_ = c.topicsMgr.Unsubscribe([]byte(sub.topic), sub)
			_ = c.session.RemoveTopic(topic)
			delete(c.subMap, topic)
		}
		c.subMapMu.Unlock()

	}

	unsuback := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	unsuback.MessageID = packet.MessageID

	err := c.WriterPacket(unsuback)
	if err != nil {
		log.Error("send unsuback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}
	// //process ubsubscribe message
	b.BroadcastSubOrUnsubMessage(packet)
}

func (c *client) ProcessPing() {
	if c.status == Disconnected {
		return
	}
	resp := packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
	err := c.WriterPacket(resp)
	if err != nil {
		log.Error("send PingResponse error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}
}

func (c *client) Close() {

	c.cancelFunc()

	if c.status == Disconnected {
		return
	}

	c.status = Disconnected

	b := c.broker
	b.Publish(&bridge.Elements{
		ClientID:  c.info.clientID,
		Username:  c.info.username,
		Action:    bridge.Disconnect,
		Timestamp: time.Now().Unix(),
	})

	if c.mu.Lock(); c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
		c.mu.Unlock()
	}

	if b == nil {
		return
	}

	b.removeClient(c)

	c.subMapMu.RLock()
	defer c.subMapMu.RUnlock()

	unSubTopics := make([]string, 0)
	for topic, sub := range c.subMap {
		unSubTopics = append(unSubTopics, topic)

		// guard against race condition where a client gets Close() but wasn't initialized yet fully
		if sub == nil || b.topicsMgr == nil {
			continue
		}

		if err := b.topicsMgr.Unsubscribe([]byte(sub.topic), sub); err != nil {
			log.Error("unsubscribe error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		}
	}

	if c.category == CLIENT {
		b.BroadcastUnSubscribe(unSubTopics)
		//offline notification
		b.OnlineOfflineNotification(c.info.clientID, false)
	}

	if c.info.willMsg != nil {
		b.PublishMessage(c.info.willMsg)
	}

	if c.category == CLUSTER {
		b.ConnectToDiscovery()
	}

	//do reconnect
	if c.category == REMOTE {
		go b.connectRouter(c.route.remoteID, c.route.remoteUrl)
	}

}

func (c *client) WriterPacket(packet packets.ControlPacket) error {
	defer func() {
		if err := recover(); err != nil {
			log.Error("recover error, ", zap.Any("recover", r))
		}
	}()
	if c.status == Disconnected {
		return nil
	}

	if packet == nil {
		return nil
	}
	if c.conn == nil {
		c.Close()
		return errors.New("connect lost ....")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	return packet.Write(c.conn)
}

// registerPublishPacketId stores the current packet ID into a maps of all
// packet ID's used by client
func (c *client) registerPublishPacketId(packetId uint16) error {
	if c.isAwaitingFull() {
		log.Error("Dropped qos2 packet for too many awaiting_rel", zap.Uint16("id", packetId))
		return errors.New("DROPPED_QOS2_PACKET_FOR_TOO_MANY_AWAITING_REL")
	}

	c.awaitingRelMu.Lock()
	defer c.awaitingRelMu.Unlock()

	// checks if packetID is already in use
	if _, found := c.awaitingRel[packetId]; found {
		return errors.New("RC_PACKET_IDENTIFIER_IN_USE")
	}

	// associates the packet id
	c.awaitingRel[packetId] = time.Now().Unix()
	time.AfterFunc(time.Duration(awaitRelTimeout)*time.Second, c.expireAwaitingRel)

	return nil
}

func (c *client) isAwaitingFull() bool {
	c.awaitingRelMu.RLock()
	defer c.awaitingRelMu.RUnlock()
	if c.maxAwaitingRel == 0 {
		return false
	}
	if len(c.awaitingRel) < c.maxAwaitingRel {
		return false
	}
	return true
}

func (c *client) expireAwaitingRel() {
	c.awaitingRelMu.Lock()
	defer c.awaitingRelMu.Unlock()
	if len(c.awaitingRel) == 0 {
		return
	}
	now := time.Now().Unix()
	for packetId, Timestamp := range c.awaitingRel {
		if now-Timestamp >= awaitRelTimeout {
			log.Error("Dropped qos2 packet for await_rel_timeout", zap.Uint16("id", packetId))
			delete(c.awaitingRel, packetId)
		}
	}
}

func (c *client) pubRel(packetId uint16) error {
	c.awaitingRelMu.Lock()
	defer c.awaitingRelMu.Unlock()
	if _, found := c.awaitingRel[packetId]; found {
		delete(c.awaitingRel, packetId)
	} else {
		log.Error("The PUBREL PacketId is not found", zap.Uint16("id", packetId))
		return errors.New("RC_PACKET_IDENTIFIER_NOT_FOUND")
	}
	return nil
}
