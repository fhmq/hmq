package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/eapache/queue"
	mqtt5 "github.com/eclipse/paho.golang/packets"
	"github.com/fhmq/hmq/v4/broker/lib/sessions"
	"github.com/fhmq/hmq/v4/broker/lib/topics"
	"github.com/fhmq/hmq/v4/broker/packets"
	"github.com/fhmq/hmq/v4/plugins/bridge"
	"go.uber.org/zap"
	"golang.org/x/net/websocket"
)

const (
	// CLIENT is an end user.
	CLIENT = 0
)

const (
	_GroupTopicRegexp = `^\$share/([0-9a-zA-Z_-]+)/(.*)$`
)

const (
	Connected    = 1
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
	typ              int
	mu               sync.Mutex
	writeMu          sync.Mutex
	outboundSendMu   sync.Mutex
	writing          bool
	writeInterrupted bool
	takeoverWaiters  int
	stateMu          sync.Mutex
	closeOnce        sync.Once
	broker           *Broker
	conn             net.Conn
	info             info
	status           int
	ctx              context.Context
	cancelFunc       context.CancelFunc
	session          *sessions.Session
	transport        net.Conn
	mqtt5            *mqtt5State
	outboundOwner    uint64
	durableEpoch     uint64
	durableReady     bool     // Guarded by outboundSendMu; replay precedes queue pumping.
	durableReplayIDs []uint16 // ReadInflight sequence order, populated before owner claim.
	subMap           map[string]*subscription
	subMapMu         sync.RWMutex
	topicsMgr        *topics.Manager
	subs             []interface{}
	qoss             []byte
	rmsgs            []*packets.PublishPacket
	awaitingRel      map[uint16]int64
	awaitingRelMu    sync.RWMutex
	publishMu        sync.Mutex
	maxAwaitingRel   int
	inflight         map[uint16]*inflightElem
	inflightMu       sync.RWMutex
	mqueue           *queue.Queue
	retryTimer       *time.Timer
	retryTimerLock   sync.Mutex
	lastMsgTime      int64
}

type InflightStatus uint8

const (
	Publish InflightStatus = 0
	Pubrel  InflightStatus = 1
)

type inflightElem struct {
	status    InflightStatus
	packet    *packets.PublishPacket
	timestamp int64
}
type subscription struct {
	client            *client
	topic             string
	qos               byte
	share             bool
	groupName         string
	generation        uint64
	noLocal           bool
	retainAsPublished bool
	subscriptionID    int
}

type info struct {
	clientID  string
	username  string
	keepalive uint16
	willMsg   *packets.PublishPacket
	remoteIP  string
}

type PubPacket struct {
	TopicName string `json:"topicName"`
	Payload   []byte `json:"payload"`
}

type Info struct {
	ClientID  string    `json:"clientId"`
	Username  string    `json:"username"`
	Keepalive uint16    `json:"keepalive"`
	WillMsg   PubPacket `json:"willMsg"`
}

var (
	DisconnectedPacket = packets.NewControlPacket(packets.Disconnect).(*packets.DisconnectPacket)
)

func (c *client) init() {
	c.transport = c.conn
	c.lastMsgTime = time.Now().Unix() //mark the connection packet time as last time messaged
	c.status = Connected
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
	c.awaitingRel = make(map[uint16]int64)
	c.inflight = make(map[uint16]*inflightElem)
	c.mqueue = queue.New()
}

func (c *client) readLoop() {
	c.mu.Lock()
	nc := c.conn
	c.mu.Unlock()
	b := c.broker
	if nc == nil || b == nil {
		return
	}

	keepAlive := time.Second * time.Duration(c.info.keepalive)
	timeOut := keepAlive + (keepAlive / 2)

	for {
		select {
		case <-c.ctx.Done():
			return
		default:
			//add read timeout
			if keepAlive > 0 {
				if err := nc.SetReadDeadline(time.Now().Add(timeOut)); err != nil {
					log.Error("set read timeout error: ", zap.Error(err), zap.String("ClientID", c.info.clientID))
					msg := &Message{
						client: c,
						packet: DisconnectedPacket,
					}
					b.SubmitWork(c.info.clientID, msg)
					return
				}
			}

			packet, v5, err := c.readClientPacket(nc)
			if err != nil {
				if c.mqtt5 != nil && !errors.Is(err, io.EOF) {
					var network net.Error
					if errors.As(err, &network) {
						if network.Timeout() {
							c.mqtt5Disconnect(0x8d)
						}
					} else {
						c.mqtt5Disconnect(mqtt5Reason(err))
					}
				}
				log.Error("read packet error: ", zap.Error(err), zap.String("ClientID", c.info.clientID))
				msg := &Message{
					client: c,
					packet: DisconnectedPacket,
				}
				b.SubmitWork(c.info.clientID, msg)
				return
			}

			// if packet is disconnect from client, then need to break the read packet loop and clear will msg.
			if _, isDisconnect := packet.(*packets.DisconnectPacket); isDisconnect {
				c.stateMu.Lock()
				if v5 == nil || v5.Content.(*mqtt5.Disconnect).ReasonCode == 0 {
					c.info.willMsg = nil
				}
				c.stateMu.Unlock()
				c.cancelFunc()
			} else {
				c.stateMu.Lock()
				c.lastMsgTime = time.Now().Unix()
				c.stateMu.Unlock()
			}

			msg := &Message{
				client: c,
				packet: packet,
				mqtt5:  v5,
			}
			b.SubmitWork(c.info.clientID, msg)
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
	case *packets.PublishPacket:
		packet := msgPacket.(*packets.PublishPacket)
		fields = append(fields, packet.TopicName)
		break

	case *packets.SubscribePacket:
		packet := msgPacket.(*packets.SubscribePacket)
		fields = append(fields, packet.Topics...)
	case *packets.SubackPacket:
	case *packets.UnsubscribePacket:
		packet := msgPacket.(*packets.UnsubscribePacket)
		fields = append(fields, packet.Topics...)
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
		if strings.ContainsRune(field, '\u0000') {
			validFields = false
			return
		}
	}

	// All fields have been validated successfully
	validFields = true

	return
}

func ProcessMessage(msg *Message) {
	c := msg.client
	closeAfterRoute := false
	defer func() {
		if closeAfterRoute {
			c.Close()
		}
	}()
	ca := msg.packet
	if ca == nil {
		return
	}
	if msg.mqtt5 != nil && c.processMQTT5NegativeAck(msg.mqtt5) {
		return
	}
	if c.durable() {
		switch ca.(type) {
		case *packets.PubackPacket, *packets.PubrecPacket, *packets.PubcompPacket:
			unlock := c.broker.lockClientConnect(c.info.clientID)
			defer unlock()
			current, ok := c.broker.clients.Load(c.info.clientID)
			if !ok || current != c {
				return
			}
		}
	}

	if c.typ == CLIENT {
		log.Debug("Recv message:", zap.String("message type", reflect.TypeOf(msg.packet).String()[9:]), zap.String("ClientID", c.info.clientID))
	}

	// Perform field validation
	if !validatePacketFields(ca) {

		// http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.pdf
		// Page 14
		//
		// If a Server or Client receives a Control Packet
		// containing ill-formed UTF-8 it MUST close the Network Connection

		c.mu.Lock()
		conn := c.conn
		c.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}

		// Update client status
		//c.status = Disconnected

		log.Error("Client disconnected due to malformed packet", zap.String("ClientID", c.info.clientID))

		return
	}

	switch ca.(type) {
	case *packets.ConnackPacket:
	case *packets.ConnectPacket:
	case *packets.PublishPacket:
		packet := ca.(*packets.PublishPacket)

		c.ProcessPublish(packet)
	case *packets.PubackPacket:
		packet := ca.(*packets.PubackPacket)
		c.outboundSendMu.Lock()
		c.inflightMu.Lock()
		if elem, found := c.inflight[packet.MessageID]; found && elem.packet.Qos == QosAtLeastOnce && elem.status == Publish {
			if err := c.persistOutbound(packet.MessageID, nil); err != nil {
				c.inflightMu.Unlock()
				c.outboundSendMu.Unlock()
				closeAfterRoute = true
				return
			}
			if c.session != nil {
				c.session.UpdateOutbound(c.outboundOwner, packet.MessageID, nil)
			}
			delete(c.inflight, packet.MessageID)
		} else {
			log.Error("Duplicated PUBACK PacketId", zap.Uint16("MessageID", packet.MessageID))
		}
		c.inflightMu.Unlock()
		c.outboundSendMu.Unlock()
		c.pumpMQTT5Queue()
		if c.durable() {
			if err := c.pumpSessionQueue(); err != nil {
				closeAfterRoute = true
			}
		}
	case *packets.PubrecPacket:
		packet := ca.(*packets.PubrecPacket)
		c.outboundSendMu.Lock()
		c.inflightMu.Lock()
		ielem, found := c.inflight[packet.MessageID]
		valid := found && ielem.packet.Qos == QosExactlyOnce
		if valid {
			if c.session != nil {
				valid = c.session.UpdateOutbound(c.outboundOwner, packet.MessageID, &sessions.OutboundMessage{Packet: ielem.packet, AwaitingPubcomp: true})
			}
		}
		if valid {
			if ielem.status == Publish {
				if err := c.persistOutbound(packet.MessageID, &sessions.OutboundMessage{Packet: ielem.packet, AwaitingPubcomp: true}); err != nil {
					c.inflightMu.Unlock()
					c.outboundSendMu.Unlock()
					closeAfterRoute = true
					return
				}
				ielem.status = Pubrel
				ielem.timestamp = time.Now().Unix()
			} else if ielem.status == Pubrel {
				log.Error("Duplicated PUBREC PacketId", zap.Uint16("MessageID", packet.MessageID))
			}
		} else {
			log.Error("The PUBREC PacketId is not found.", zap.Uint16("MessageID", packet.MessageID))
		}
		c.inflightMu.Unlock()
		if !valid {
			c.outboundSendMu.Unlock()
			if c.mqtt5 != nil {
				_ = c.WriterPacket(&mqtt5WirePacket{wire: &mqtt5.Pubrel{PacketID: packet.MessageID, ReasonCode: 0x92, Properties: &mqtt5.Properties{}}})
			}
			return
		}

		pubrel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
		pubrel.MessageID = packet.MessageID
		err := c.WriterPacket(pubrel)
		c.outboundSendMu.Unlock()
		if err != nil {
			log.Error("send pubrel error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			return
		}
	case *packets.PubrelPacket:
		if c.broker.quorumEnabled() {
			closeAfterRoute = true
			return
		}
		packet := ca.(*packets.PubrelPacket)
		relErr := c.pubRel(packet.MessageID)
		if c.mqtt5 != nil && relErr != nil {
			_ = c.WriterPacket(&mqtt5WirePacket{wire: &mqtt5.Pubcomp{PacketID: packet.MessageID, ReasonCode: 0x92, Properties: &mqtt5.Properties{}}})
			return
		}
		pubcomp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
		pubcomp.MessageID = packet.MessageID
		if err := c.WriterPacket(pubcomp); err != nil {
			log.Error("send pubcomp error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			return
		}
	case *packets.PubcompPacket:
		packet := ca.(*packets.PubcompPacket)
		c.outboundSendMu.Lock()
		c.inflightMu.Lock()
		if elem, found := c.inflight[packet.MessageID]; found && elem.packet.Qos == QosExactlyOnce && elem.status == Pubrel {
			if err := c.persistOutbound(packet.MessageID, nil); err != nil {
				c.inflightMu.Unlock()
				c.outboundSendMu.Unlock()
				closeAfterRoute = true
				return
			}
			if c.session != nil {
				c.session.UpdateOutbound(c.outboundOwner, packet.MessageID, nil)
			}
			delete(c.inflight, packet.MessageID)
		}
		c.inflightMu.Unlock()
		c.outboundSendMu.Unlock()
		c.pumpMQTT5Queue()
		if c.durable() {
			if err := c.pumpSessionQueue(); err != nil {
				closeAfterRoute = true
			}
		}
	case *packets.SubscribePacket:
		packet := ca.(*packets.SubscribePacket)
		if msg.mqtt5 != nil {
			c.processClientSubscribeOptions(packet, msg.mqtt5.Content.(*mqtt5.Subscribe))
		} else {
			c.ProcessSubscribe(packet)
		}
	case *packets.SubackPacket:
	case *packets.UnsubscribePacket:
		packet := ca.(*packets.UnsubscribePacket)
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
	c.processClientPublish(packet)
}

func (c *client) processClientPublish(packet *packets.PublishPacket) {
	if c.broker.quorumEnabled() {
		c.processQuorumPublish(packet)
		return
	}
	if c.broker.sessionLog != nil && c.typ == CLIENT && packet.Qos == QosExactlyOnce {
		c.Close()
		return
	}
	c.publishMu.Lock()
	defer c.publishMu.Unlock()

	topic := packet.TopicName

	if !c.broker.CheckTopicAuth(PUB, c.info.clientID, c.info.username, c.info.remoteIP, topic) {
		log.Error("Pub Topics Auth failed, ", zap.String("topic", topic), zap.String("ClientID", c.info.clientID))
		if c.mqtt5 != nil {
			if packet.Qos == 1 {
				_ = c.WriterPacket(&mqtt5WirePacket{wire: &mqtt5.Puback{PacketID: packet.MessageID, ReasonCode: 0x87, Properties: &mqtt5.Properties{}}})
			} else if packet.Qos == 2 {
				_ = c.WriterPacket(&mqtt5WirePacket{wire: &mqtt5.Pubrec{PacketID: packet.MessageID, ReasonCode: 0x87, Properties: &mqtt5.Properties{}}})
			}
		}
		return
	}
	registered := false
	defer func() {
		if registered {
			if c.session != nil {
				c.session.RemoveAwaitingRel(packet.MessageID)
				return
			}
			c.awaitingRelMu.Lock()
			delete(c.awaitingRel, packet.MessageID)
			c.awaitingRelMu.Unlock()
		}
	}()
	if packet.Qos == QosExactlyOnce {
		err := c.registerPublishPacketId(packet.MessageID)
		if err != nil && err.Error() != "RC_PACKET_IDENTIFIER_IN_USE" {
			c.Close()
			return
		}
		if err != nil {
			pubrec := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
			pubrec.MessageID = packet.MessageID
			_ = c.WriterPacket(pubrec)
			return
		}
		registered = true
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

	if cost && packet.Qos == QosAtMostOnce {
		return
	}
	if !cost && packet.Retain {
		if err := c.topicsMgr.Retain(packet); err != nil {
			log.Error("Error retaining message", zap.Error(err), zap.String("ClientID", c.info.clientID))
			c.Close()
			return
		}
	}
	if !cost {
		if err := c.forwardPublishMessage(packet); err != nil {
			log.Error("forward publish", zap.Error(err), zap.String("ClientID", c.info.clientID))
			c.Close()
			return
		}
		if err := c.broker.publishLocal(packet); err != nil {
			log.Error("local publish", zap.Error(err), zap.String("ClientID", c.info.clientID))
			c.Close()
			return
		}
	}

	switch packet.Qos {
	case QosAtMostOnce:
	case QosAtLeastOnce:
		puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		puback.MessageID = packet.MessageID
		if err := c.WriterPacket(puback); err != nil {
			log.Error("send puback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			return
		}
	case QosExactlyOnce:
		registered = false
		{
			pubrec := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
			pubrec.MessageID = packet.MessageID
			if err := c.WriterPacket(pubrec); err != nil {
				log.Error("send pubrec error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
				return
			}
		}
		return
	default:
		log.Error("publish with unknown qos", zap.String("ClientID", c.info.clientID))
		return
	}

}

func (c *client) ProcessPublishMessage(packet *packets.PublishPacket) error {
	if c.broker.quorumEnabled() {
		return c.broker.PublishMessage(packet)
	}
	if err := c.forwardPublishMessage(packet); err != nil {
		log.Error("forward publish", zap.Error(err))
		return err
	}
	if c.broker != nil {
		return c.broker.publishLocal(packet)
	}
	return nil
}

func (c *client) forwardPublishMessage(packet *packets.PublishPacket) error {
	b := c.broker
	if b == nil {
		return nil
	}
	typ := c.typ
	if typ == CLIENT {
		return b.forwardMessage(packet)
	}
	return nil
}

func (c *client) ProcessSubscribe(packet *packets.SubscribePacket) {
	c.processClientSubscribe(packet)
}

func (c *client) processClientSubscribe(packet *packets.SubscribePacket) {
	c.processClientSubscribeOptions(packet, nil)
}

func (c *client) processClientSubscribeOptions(packet *packets.SubscribePacket, options *mqtt5.Subscribe) {
	if c.isDisconnected() {
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
	retcodes := make([]byte, 0, len(subTopics))
	type retainedDelivery struct {
		packet *packets.PublishPacket
		sub    *subscription
	}
	var retainedMsgs []retainedDelivery

	for i, topic := range subTopics {
		t := topic
		if (c.durable() || b.quorumEnabled()) && strings.HasPrefix(topic, "$share/") || (b.quorumEnabled() && qoss[i] > 1) {
			retcodes = append(retcodes, QosFailure)
			continue
		}
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

		c.stateMu.Lock()
		if c.status == Disconnected {
			c.stateMu.Unlock()
			return
		}
		c.subMapMu.Lock()
		if err := c.persistFilter(t, &qoss[i]); err != nil {
			c.subMapMu.Unlock()
			c.stateMu.Unlock()
			c.Close()
			return
		}
		generation := sessions.NewSubscriptionGeneration()
		_, existed := c.subMap[t]
		if oldSub, exist := c.subMap[t]; exist {
			generation = oldSub.generation
			_ = c.topicsMgr.Unsubscribe([]byte(oldSub.topic), oldSub)
			delete(c.subMap, t)
		}

		sub := &subscription{
			topic:      topic,
			qos:        qoss[i],
			client:     c,
			share:      share,
			groupName:  groupName,
			generation: generation,
		}
		if options != nil {
			o := options.Subscriptions[i]
			sub.noLocal = o.NoLocal
			sub.retainAsPublished = o.RetainAsPublished
			if options.Properties.SubscriptionIdentifier != nil {
				sub.subscriptionID = *options.Properties.SubscriptionIdentifier
			}
		}

		rqos, err := c.topicsMgr.Subscribe([]byte(topic), qoss[i], sub)
		if err != nil {
			c.subMapMu.Unlock()
			c.stateMu.Unlock()
			log.Error("subscribe error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			retcodes = append(retcodes, QosFailure)
			continue
		}

		c.subMap[t] = sub
		_ = c.session.AddTopicGeneration(t, qoss[i], generation)
		c.subMapMu.Unlock()
		c.stateMu.Unlock()
		retcodes = append(retcodes, rqos)
		if options != nil && (sub.share || options.Subscriptions[i].RetainHandling == 2 || options.Subscriptions[i].RetainHandling == 1 && existed) {
			continue
		}
		var matches []*packets.PublishPacket
		if err := c.topicsMgr.Retained([]byte(topic), &matches); err != nil {
			log.Error("read retained messages", zap.Error(err), zap.String("ClientID", c.info.clientID))
			c.Close()
			return
		}
		for _, msg := range matches {
			retainedMsgs = append(retainedMsgs, retainedDelivery{msg, sub})
		}
	}

	suback.ReturnCodes = retcodes

	err := c.WriterPacket(suback)
	if err != nil {
		log.Error("send suback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}

	//process retain message
	for _, rm := range retainedMsgs {
		qos := rm.packet.Qos
		if rm.sub.qos < qos {
			qos = rm.sub.qos
		}
		if err := publishDelivery(rm.sub, rm.packet, qos, true); err != nil {
			log.Error("deliver retained message", zap.Error(err), zap.String("ClientID", c.info.clientID))
		}
	}
}

// restoreSubscriptions reattaches saved filters without replaying retained messages.
func (c *client) restoreSubscriptions() error {
	filters, qoss, err := c.session.Topics()
	if err != nil {
		return err
	}
	for i, filter := range filters {
		topic, group, shared := filter, "", false
		if strings.HasPrefix(filter, "$share/") {
			parts := groupCompile.FindStringSubmatch(filter)
			if len(parts) != 3 {
				return fmt.Errorf("invalid saved shared subscription %q", filter)
			}
			topic, group, shared = parts[2], parts[1], true
		}
		sub := &subscription{client: c, topic: topic, qos: qoss[i], share: shared, groupName: group, generation: c.session.TopicGeneration(filter)}
		if _, err := c.topicsMgr.Subscribe([]byte(topic), qoss[i], sub); err != nil {
			return err
		}
		c.subMap[filter] = sub
	}
	return nil
}

func (c *client) ProcessUnSubscribe(packet *packets.UnsubscribePacket) {
	c.processClientUnSubscribe(packet)
}

func (c *client) processClientUnSubscribe(packet *packets.UnsubscribePacket) {
	if c.isDisconnected() {
		return
	}
	b := c.broker
	if b == nil {
		return
	}

	unSubTopics := packet.Topics
	reasons := make([]byte, 0, len(unSubTopics))

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

		c.stateMu.Lock()
		if c.status == Disconnected {
			c.stateMu.Unlock()
			return
		}
		c.subMapMu.Lock()
		sub, exist := c.subMap[topic]
		if exist {
			reasons = append(reasons, 0)
		} else {
			reasons = append(reasons, 0x11)
		}
		if exist {
			if err := c.persistFilter(topic, nil); err != nil {
				c.subMapMu.Unlock()
				c.stateMu.Unlock()
				c.Close()
				return
			}
			_ = c.topicsMgr.Unsubscribe([]byte(sub.topic), sub)
			_ = c.session.RemoveTopic(topic)
			delete(c.subMap, topic)
		}
		c.subMapMu.Unlock()
		c.stateMu.Unlock()

	}

	unsuback := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	unsuback.MessageID = packet.MessageID

	var err error
	if c.mqtt5 != nil {
		err = c.WriterPacket(&mqtt5WirePacket{wire: &mqtt5.Unsuback{PacketID: packet.MessageID, Reasons: reasons, Properties: &mqtt5.Properties{}}})
	} else {
		err = c.WriterPacket(unsuback)
	}
	if err != nil {
		log.Error("send unsuback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}
}

func (c *client) ProcessPing() {
	if c.isDisconnected() {
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
	if notify := c.detach(); notify != nil {
		notify()
	}
}

func (c *client) detach() (notify func()) {
	c.closeOnce.Do(func() { notify = c.close() })
	return notify
}

func (c *client) isDisconnected() bool {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.status == Disconnected
}

func (c *client) close() func() {
	if c.cancelFunc != nil {
		c.cancelFunc()
	}

	c.stateMu.Lock()
	c.status = Disconnected
	willMsg, lastMsgTime := c.info.willMsg, c.lastMsgTime
	c.stateMu.Unlock()
	c.retryTimerLock.Lock()
	if c.retryTimer != nil {
		c.retryTimer.Stop()
		c.retryTimer = nil
	}
	c.retryTimerLock.Unlock()
	c.inflightMu.Lock()
	clear(c.inflight)
	c.inflightMu.Unlock()
	//wait for message complete
	// time.Sleep(1 * time.Second)
	// c.status = Disconnected

	b := c.broker

	c.mu.Lock()
	conn := c.conn
	c.conn = nil
	c.mu.Unlock()

	if b == nil {
		return func() {
			if c.transport != nil {
				_ = c.transport.Close()
			}
			if conn != nil {
				_ = conn.Close()
			}
		}
	}

	b.removeClient(c)

	c.subMapMu.RLock()

	for _, sub := range c.subMap {

		// guard against race condition where a client gets Close() but wasn't initialized yet fully
		if sub == nil || b.topicsMgr == nil {
			continue
		}

		if err := b.topicsMgr.Unsubscribe([]byte(sub.topic), sub); err != nil {
			log.Error("unsubscribe error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		}
	}
	c.subMapMu.RUnlock()

	return func() {
		if c.transport != nil {
			_ = c.transport.Close()
		}
		if conn != nil {
			_ = conn.Close()
		}
		b.Publish(&bridge.Elements{ClientID: c.info.clientID, Username: c.info.username, Action: bridge.Disconnect, Timestamp: time.Now().Unix()})
		if c.typ == CLIENT {
			var pubPack = PubPacket{}
			if willMsg != nil {
				pubPack.TopicName = willMsg.TopicName
				pubPack.Payload = willMsg.Payload
			}

			pubInfo := Info{
				ClientID:  c.info.clientID,
				Username:  c.info.username,
				Keepalive: c.info.keepalive,
				WillMsg:   pubPack,
			}
			//offline notification
			b.OnlineOfflineNotification(pubInfo, false, lastMsgTime)
		}

		if willMsg != nil {
			if willMsg.Metadata != nil && willMsg.Metadata.Properties != nil && willMsg.Metadata.Properties.MessageExpiry != nil {
				willMsg.Metadata.ExpiresAt = time.Now().Add(time.Duration(*willMsg.Metadata.Properties.MessageExpiry) * time.Second)
			}
			b.PublishMessage(willMsg)
		}
	}

}

var errTakeoverWrite = errors.New("write interrupted by client takeover")
var errTakeoverWriteStarted = errors.New("interrupted write may have sent a partial packet")
var errWriteNotStarted = errors.New("packet write not started")
var errWriteZeroBytes = errors.New("packet write sent zero bytes")

type countingPacketWriter struct {
	conn    net.Conn
	written int
}

func (w *countingPacketWriter) Write(p []byte) (int, error) {
	n, err := w.conn.Write(p)
	w.written += n
	return n, err
}

func (c *client) WriterPacket(packet packets.ControlPacket) (result error) {
	defer func() {
		if err := recover(); err != nil {
			log.Error("recover error, ", zap.Any("recover", err))
			result = fmt.Errorf("write packet panic: %v", err)
		}
	}()
	if packet == nil {
		return nil
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.isDisconnected() {
		return errors.Join(net.ErrClosed, errWriteNotStarted)
	}
	c.mu.Lock()
	conn := c.conn
	if c.takeoverWaiters != 0 {
		c.mu.Unlock()
		return errors.Join(errTakeoverWrite, errWriteNotStarted)
	}
	if conn == nil {
		c.mu.Unlock()
		return errors.Join(net.ErrClosed, errWriteNotStarted)
	}
	if err := conn.SetWriteDeadline(time.Now().Add(2 * time.Second)); err != nil {
		c.mu.Unlock()
		return err
	}
	c.writing = true
	c.writeInterrupted = false
	c.mu.Unlock()
	w := &countingPacketWriter{conn: conn}
	defer func() {
		c.mu.Lock()
		if result != nil && c.writeInterrupted && errors.Is(result, os.ErrDeadlineExceeded) {
			result = errors.Join(result, errTakeoverWrite, errTakeoverWriteStarted)
			if w.written == 0 {
				result = errors.Join(result, errWriteZeroBytes)
			}
		}
		c.writing = false
		_ = conn.SetWriteDeadline(time.Time{})
		c.mu.Unlock()
	}()
	return c.writeClientPacket(w, packet)
}

// beginTakeover stops queued writes and wakes an active write before acquiring the route lock.
func (c *client) beginTakeover() func() {
	c.mu.Lock()
	c.takeoverWaiters++
	if c.conn != nil && c.writing {
		if err := c.conn.SetWriteDeadline(time.Now()); err == nil {
			c.writeInterrupted = true
		}
	}
	c.mu.Unlock()
	return func() {
		c.mu.Lock()
		c.takeoverWaiters--
		c.mu.Unlock()
	}
}

func (c *client) registerPublishPacketId(packetId uint16) error {
	if c.session != nil {
		switch c.session.RegisterAwaitingRel(packetId, time.Now().Unix(), c.maxAwaitingRel) {
		case 1:
			return errors.New("RC_PACKET_IDENTIFIER_IN_USE")
		case 2:
			return errors.New("DROPPED_QOS2_PACKET_FOR_TOO_MANY_AWAITING_REL")
		default:
			return nil
		}
	}
	c.awaitingRelMu.Lock()
	defer c.awaitingRelMu.Unlock()
	if _, found := c.awaitingRel[packetId]; found {
		return errors.New("RC_PACKET_IDENTIFIER_IN_USE")
	}
	if c.maxAwaitingRel > 0 && len(c.awaitingRel) >= c.maxAwaitingRel {
		log.Error("Dropped qos2 packet for too many awaiting_rel", zap.Uint16("id", packetId))
		return errors.New("DROPPED_QOS2_PACKET_FOR_TOO_MANY_AWAITING_REL")
	}

	c.awaitingRel[packetId] = time.Now().Unix()
	return nil
}

func (c *client) isAwaitingFull() bool {
	if c.session != nil {
		return c.maxAwaitingRel > 0 && c.session.AwaitingRelCount() >= c.maxAwaitingRel
	}
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
	// Session state must survive until PUBREL or session deletion.
	if c.session != nil {
		return
	}
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
	if c.session != nil {
		if c.session.RemoveAwaitingRel(packetId) {
			return nil
		}
		return errors.New("RC_PACKET_IDENTIFIER_NOT_FOUND")
	}
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
