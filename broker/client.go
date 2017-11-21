package broker

import (
	"net"
	"strings"
	"sync"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"

	log "github.com/cihub/seelog"
)

const (
	// special pub topic for cluster info BrokerInfoTopic
	BrokerInfoTopic = "broker001info/brokerinfo"
	// CLIENT is an end user.
	CLIENT = 0
	// ROUTER is another router in the cluster.
	ROUTER = 1
	//REMOTE is the router connect to other cluster
	REMOTE = 2
)
const (
	Connected    = 1
	Disconnected = 2
)

type client struct {
	typ    int
	mu     sync.Mutex
	broker *Broker
	conn   net.Conn
	info   info
	route  *route
	status int
	smu    sync.RWMutex
	mp     *MessagePool
	subs   map[string]*subscription
	rsubs  map[string]*subInfo
}

type subInfo struct {
	sub *subscription
	num int
}

type subscription struct {
	client *client
	topic  string
	qos    byte
	queue  bool
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
	DisconnectdPacket = packets.NewControlPacket(packets.Disconnect).(*packets.DisconnectPacket)
)

func (c *client) init() {
	c.smu.Lock()
	defer c.smu.Unlock()
	c.status = Connected
	typ := c.typ
	if typ == ROUTER {
		c.rsubs = make(map[string]*subInfo)
	} else if typ == CLIENT {
		c.subs = make(map[string]*subscription, 10)
	}
	c.info.localIP = strings.Split(c.conn.LocalAddr().String(), ":")[0]
	c.info.remoteIP = strings.Split(c.conn.RemoteAddr().String(), ":")[0]
}

func (c *client) readLoop() {
	nc := c.conn
	msgPool := c.mp
	if nc == nil || msgPool == nil {
		return
	}

	lastIn := uint16(time.Now().Unix())
	var nowTime uint16
	for {
		nowTime = uint16(time.Now().Unix())
		if 0 != c.info.keepalive && nowTime-lastIn > c.info.keepalive*3/2 {
			log.Errorf("Client %s has exceeded timeout, disconnecting.\n", c.info.clientID)
			break
		}
		packet, err := packets.ReadPacket(nc)
		if err != nil {
			log.Error("read packet error: ", err, " clientID = ", c.info.clientID)
			break
		}
		// log.Info("recv buf: ", packet)
		lastIn = uint16(time.Now().Unix())
		msg := &Message{
			client: c,
			packet: packet,
		}
		msgPool.queue <- msg
	}
	msg := &Message{client: c, packet: DisconnectdPacket}
	msgPool.queue <- msg
	msgPool.Reduce()
}

func ProcessMessage(msg *Message) {
	c := msg.client
	ca := msg.packet
	if ca == nil {
		return
	}
	log.Debug("Recv message: ", ca.String(), " clientID = ", c.info.clientID)
	switch ca.(type) {
	case *packets.ConnackPacket:

	case *packets.ConnectPacket:
	case *packets.PublishPacket:
		packet := ca.(*packets.PublishPacket)
		c.ProcessPublish(packet)
	case *packets.PubackPacket:
	case *packets.PubrecPacket:
	case *packets.PubrelPacket:
	case *packets.PubcompPacket:
	case *packets.SubscribePacket:
		packet := ca.(*packets.SubscribePacket)
		c.ProcessSubscribe(packet)
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
		log.Info("Recv Unknow message.......", " clientID = ", c.info.clientID)
	}
}

func (c *client) ProcessPublish(packet *packets.PublishPacket) {
	if c.status == Disconnected {
		return
	}

	topic := packet.TopicName
	if !c.CheckTopicAuth(PUB, topic) {
		log.Error("Pub Topics Auth failed, ", topic, " clientID = ", c.info.clientID)
		return
	}

	switch packet.Qos {
	case QosAtMostOnce:
		c.ProcessPublishMessage(packet)
	case QosAtLeastOnce:
		puback := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		puback.MessageID = packet.MessageID
		if err := c.WriterPacket(puback); err != nil {
			log.Error("send puback error, ", err, " clientID = ", c.info.clientID)
			return
		}
		c.ProcessPublishMessage(packet)
	case QosExactlyOnce:
		return
	default:
		log.Error("publish with unknown qos", " clientID = ", c.info.clientID)
		return
	}
	if packet.Retain {
		if b := c.broker; b != nil {
			err := b.rl.Insert(topic, packet)
			if err != nil {
				log.Error("Insert Retain Message error: ", err, " clientID = ", c.info.clientID)
			}
		}
	}

}

func (c *client) ProcessPublishMessage(packet *packets.PublishPacket) {
	if c.status == Disconnected {
		return
	}

	b := c.broker
	if b == nil {
		return
	}
	typ := c.typ
	topic := packet.TopicName

	r := b.sl.Match(topic)
	// log.Info("psubs num: ", len(r.psubs))
	if len(r.qsubs) == 0 && len(r.psubs) == 0 {
		return
	}

	for _, sub := range r.psubs {
		if sub.client.typ == ROUTER {
			if typ == ROUTER {
				continue
			}
		}
		if sub != nil {
			err := sub.client.WriterPacket(packet)
			if err != nil {
				log.Error("process message for psub error,  ", err, " clientID = ", c.info.clientID)
			}
		}
	}

	pre := -1
	now := -1
	t := "$queue/" + topic
	cnt, exist := b.queues[t]
	if exist {
		// log.Info("queue index : ", cnt)
		for _, sub := range r.qsubs {
			if sub.client.typ == ROUTER {
				if c.typ == ROUTER {
					continue
				}
			}
			if c.typ == CLIENT {
				now = now + 1
			} else {
				now = now + sub.client.rsubs[t].num
			}
			if cnt > pre && cnt <= now {
				if sub != nil {
					err := sub.client.WriterPacket(packet)
					if err != nil {
						log.Error("send publish error, ", err, " clientID = ", c.info.clientID)
					}
				}

				break
			}
			pre = now
		}
	}

	length := getQueueSubscribeNum(r.qsubs)
	if length > 0 {
		b.queues[t] = (b.queues[t] + 1) % length
	}
}

func getQueueSubscribeNum(qsubs []*subscription) int {
	topic := "$queue/"
	if len(qsubs) < 1 {
		return 0
	} else {
		topic = topic + qsubs[0].topic
	}
	num := 0
	for _, sub := range qsubs {
		if sub.client.typ == CLIENT {
			num = num + 1
		} else {
			num = num + sub.client.rsubs[topic].num
		}
	}
	return num
}

func (c *client) ProcessSubscribe(packet *packets.SubscribePacket) {
	if c.status == Disconnected {
		return
	}

	b := c.broker
	if b == nil {
		return
	}
	topics := packet.Topics
	qoss := packet.Qoss

	suback := packets.NewControlPacket(packets.Suback).(*packets.SubackPacket)
	suback.MessageID = packet.MessageID
	var retcodes []byte

	for i, topic := range topics {
		t := topic
		//check topic auth for client
		if !c.CheckTopicAuth(SUB, topic) {
			log.Error("Sub topic Auth failed: ", topic, " clientID = ", c.info.clientID)
			retcodes = append(retcodes, QosFailure)
			continue
		}

		queue := strings.HasPrefix(topic, "$queue/")
		if queue {
			if len(t) > 7 {
				t = t[7:]
				if _, exists := b.queues[topic]; !exists {
					b.queues[topic] = 0
				}
			} else {
				retcodes = append(retcodes, QosFailure)
				continue
			}
		}
		sub := &subscription{
			topic:  t,
			qos:    qoss[i],
			client: c,
			queue:  queue,
		}
		switch c.typ {
		case CLIENT:
			if _, exist := c.subs[topic]; !exist {
				c.subs[topic] = sub

			} else {
				//if exist ,check whether qos change
				c.subs[topic].qos = qoss[i]
				retcodes = append(retcodes, qoss[i])
				continue
			}
		case ROUTER:
			if subinfo, exist := c.rsubs[topic]; !exist {
				sinfo := &subInfo{sub: sub, num: 1}
				c.rsubs[topic] = sinfo

			} else {
				subinfo.num = subinfo.num + 1
				retcodes = append(retcodes, qoss[i])
				continue
			}
		}
		err := b.sl.Insert(sub)
		if err != nil {
			log.Error("Insert subscription error: ", err, " clientID = ", c.info.clientID)
			retcodes = append(retcodes, QosFailure)
		} else {
			retcodes = append(retcodes, qoss[i])
		}
	}
	suback.ReturnCodes = retcodes

	err := c.WriterPacket(suback)
	if err != nil {
		log.Error("send suback error, ", err, " clientID = ", c.info.clientID)
		return
	}
	//broadcast subscribe message
	if c.typ == CLIENT {
		go b.BroadcastSubOrUnsubMessage(packet)
	}

	//process retain message
	for _, t := range topics {
		packets := b.rl.Match(t)
		for _, packet := range packets {
			log.Info("process retain  message: ", packet, " clientID = ", c.info.clientID)
			if packet != nil {
				c.WriterPacket(packet)
			}
		}
	}
}

func (c *client) ProcessUnSubscribe(packet *packets.UnsubscribePacket) {
	if c.status == Disconnected {
		return
	}
	b := c.broker
	if b == nil {
		return
	}
	typ := c.typ
	topics := packet.Topics

	for _, t := range topics {
		var sub *subscription
		ok := false
		switch typ {
		case CLIENT:
			sub, ok = c.subs[t]
		case ROUTER:
			subinfo, ok := c.rsubs[t]
			if ok {
				subinfo.num = subinfo.num - 1
				if subinfo.num < 1 {
					sub = subinfo.sub
					delete(c.rsubs, t)
				} else {
					c.rsubs[t] = subinfo
					sub = nil
				}
			} else {
				return
			}
		}
		if ok {
			go c.unsubscribe(sub)
		}

	}

	unsuback := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	unsuback.MessageID = packet.MessageID

	err := c.WriterPacket(unsuback)
	if err != nil {
		log.Error("send unsuback error, ", err, " clientID = ", c.info.clientID)
		return
	}
	// //process ubsubscribe message
	if c.typ == CLIENT {
		b.BroadcastSubOrUnsubMessage(packet)
	}
}

func (c *client) unsubscribe(sub *subscription) {

	if c.typ == CLIENT {
		delete(c.subs, sub.topic)

	}
	b := c.broker
	if b != nil && sub != nil {
		b.sl.Remove(sub)
	}

}

func (c *client) ProcessPing() {
	if c.status == Disconnected {
		return
	}
	resp := packets.NewControlPacket(packets.Pingresp).(*packets.PingrespPacket)
	err := c.WriterPacket(resp)
	if err != nil {
		log.Error("send PingResponse error, ", err, " clientID = ", c.info.clientID)
		return
	}
}

func (c *client) Close() {
	if c.status == Disconnected {
		return
	}
	//wait for message complete
	time.Sleep(time.Second)

	c.smu.Lock()
	c.status = Disconnected
	c.smu.Unlock()

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	b := c.broker
	subs := c.subs
	if b != nil {
		b.removeClient(c)
		for _, sub := range subs {
			err := b.sl.Remove(sub)
			if err != nil {
				log.Error("closed client but remove sublist error, ", err, " clientID = ", c.info.clientID)
			}
		}
		if c.typ == CLIENT {
			b.BroadcastUnSubscribe(subs)
		}
		if c.info.willMsg != nil {
			b.PublishMessage(c.info.willMsg)
		}

		//do reconnect
		if c.typ == REMOTE {
			b.connectRouter(c.route.remoteUrl, "")
		}
	}
}

func (c *client) WriterPacket(packet packets.ControlPacket) error {
	c.mu.Lock()
	err := packet.Write(c.conn)
	c.mu.Unlock()
	return err
}
