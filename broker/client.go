package broker

import (
	"errors"
	"hmq/lib/message"
	"net"
	"strings"
	"sync"

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

type client struct {
	typ    int
	mu     sync.Mutex
	broker *Broker
	conn   net.Conn
	info   info
	route  *route
	subs   map[string]*subscription
}

type subscription struct {
	client *client
	topic  []byte
	qos    byte
	queue  bool
}

type info struct {
	clientID  []byte
	username  []byte
	password  []byte
	keepalive uint16
	willMsg   *message.PublishMessage
	localIP   string
	remoteIP  string
}

type route struct {
	remoteID  string
	remoteUrl string
}

func (c *client) init() {
	c.subs = make(map[string]*subscription, 10)
	c.info.localIP = strings.Split(c.conn.LocalAddr().String(), ":")[0]
	c.info.remoteIP = strings.Split(c.conn.RemoteAddr().String(), ":")[0]
}

func (c *client) readLoop(msgPool *MessagePool) {
	nc := c.conn
	if nc == nil || msgPool == nil {
		return
	}
	msg := &Message{}
	for {
		buf, err := ReadPacket(nc)
		if err != nil {
			log.Error("read packet error: ", err)
			c.Close()
			return
		}
		msg.client = c
		msg.msg = buf
		msgPool.queue <- msg
	}
	msgPool.Reduce()
}

func ProcessMessage(msg *Message) {
	buf := msg.msg
	c := msg.client
	if c == nil || buf == nil {
		return
	}
	msgType := uint8(buf[0] & 0xF0 >> 4)
	switch msgType {
	case CONNACK:
		// log.Info("Recv conack message..........")
		c.ProcessConnAck(buf)
	case CONNECT:
		// log.Info("Recv connect message..........")
		c.ProcessConnect(buf)
	case PUBLISH:
		// log.Info("Recv publish message..........")
		c.ProcessPublish(buf)
	case PUBACK:
		//log.Info("Recv publish  ack message..........")
		c.ProcessPubAck(buf)
	case PUBCOMP:
		//log.Info("Recv publish  ack message..........")
		c.ProcessPubComp(buf)
	case PUBREC:
		//log.Info("Recv publish rec message..........")
		c.ProcessPubREC(buf)
	case PUBREL:
		//log.Info("Recv publish rel message..........")
		c.ProcessPubREL(buf)
	case SUBSCRIBE:
		// log.Info("Recv subscribe message.....")
		c.ProcessSubscribe(buf)
	case SUBACK:
		// log.Info("Recv suback message.....")
	case UNSUBSCRIBE:
		// log.Info("Recv unsubscribe message.....")
		c.ProcessUnSubscribe(buf)
	case UNSUBACK:
		//log.Info("Recv unsuback message.....")
	case PINGREQ:
		// log.Info("Recv PINGREQ message..........")
		c.ProcessPing(buf)
	case PINGRESP:
		//log.Info("Recv PINGRESP message..........")
	case DISCONNECT:
		// log.Info("Recv DISCONNECT message.......")
		c.Close()
	default:
		log.Info("Recv Unknow message.......")
	}
}

func (c *client) ProcessConnect(buf []byte) {

}

func (c *client) ProcessConnAck(buf []byte) {

}

func (c *client) ProcessPublish(buf []byte) {
	msg, err := DecodePublishMessage(buf)
	if err != nil {
		log.Error("Decode Publish Message error: ", err)
		c.Close()
		return
	}
	topic := msg.Topic()

	if c.typ != CLIENT || !c.CheckTopicAuth(PUB, string(topic)) {
		return
	}
	c.ProcessPublishMessage(buf, msg)

	if msg.Retain() {
		if b := c.broker; b != nil {
			err := b.rl.Insert(topic, buf)
			if err != nil {
				log.Error("Insert Retain Message error: ", err)
			}
		}
	}

}

func (c *client) ProcessPublishMessage(buf []byte, msg *message.PublishMessage) {

	b := c.broker
	if b == nil {
		return
	}
	typ := c.typ
	topic := string(msg.Topic())

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
			err := sub.client.writeBuffer(buf)
			if err != nil {
				log.Error("process message for psub error,  ", err)
			}
		}
	}

	for i, sub := range r.qsubs {
		if sub.client.typ == ROUTER {
			if typ == ROUTER {
				continue
			}
		}
		// s.qmu.Lock()
		if cnt, exist := b.queues[string(sub.topic)]; exist && i == cnt {
			if sub != nil {
				err := sub.client.writeBuffer(buf)
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

func (c *client) ProcessPubAck(buf []byte) {

}

func (c *client) ProcessPubREC(buf []byte) {

}

func (c *client) ProcessPubREL(buf []byte) {

}

func (c *client) ProcessPubComp(buf []byte) {

}

func (c *client) ProcessSubscribe(buf []byte) {
	b := c.broker
	if b == nil {
		return
	}
	msg, err := DecodeSubscribeMessage(buf)
	if err != nil {
		log.Error("Decode Subscribe Message error: ", err)
		c.Close()
		return
	}
	topics := msg.Topics()
	qos := msg.Qos()

	suback := message.NewSubackMessage()
	suback.SetPacketId(msg.PacketId())
	var retcodes []byte

	for i, t := range topics {
		topic := string(t)
		//check topic auth for client
		if c.typ == CLIENT {
			if !c.CheckTopicAuth(SUB, topic) {
				log.Error("CheckSubAuth failed")
				retcodes = append(retcodes, message.QosFailure)
				continue
			}
		}
		if _, exist := c.subs[topic]; !exist {
			queue := false
			if strings.HasPrefix(topic, "$queue/") {
				if len(t) > 7 {
					t = t[7:]
					queue = true
					// b.qmu.Lock()
					if _, exists := b.queues[topic]; !exists {
						b.queues[topic] = 0
					}
					// b.qmu.Unlock()
				} else {
					retcodes = append(retcodes, message.QosFailure)
					continue
				}
			}
			sub := &subscription{
				topic:  t,
				qos:    qos[i],
				client: c,
				queue:  queue,
			}

			c.mu.Lock()
			c.subs[topic] = sub
			c.mu.Unlock()

			err := b.sl.Insert(sub)
			if err != nil {
				log.Error("Insert subscription error: ", err)
				retcodes = append(retcodes, message.QosFailure)
			}
			retcodes = append(retcodes, qos[i])
		} else {
			//if exist ,check whether qos change
			c.subs[topic].qos = qos[i]
			retcodes = append(retcodes, qos[i])
		}

	}

	if err := suback.AddReturnCodes(retcodes); err != nil {
		log.Error("add return suback code error, ", err)
		// if typ == CLIENT {
		c.Close()
		// }
		return
	}

	err1 := c.writeMessage(suback)
	if err1 != nil {
		log.Error("send suback error, ", err1)
		return
	}
	//broadcast subscribe message
	if c.typ == CLIENT {
		go b.BroadcastSubOrUnsubMessage(buf)
	}

	//process retain message
	for _, t := range topics {
		bufs := b.rl.Match(t)
		for _, buf := range bufs {
			log.Info("process retain  message: ", string(buf))
			if buf != nil && string(buf) != "" {
				c.writeBuffer(buf)
			}
		}
	}
}

func (c *client) ProcessUnSubscribe(buf []byte) {
	b := c.broker
	if b == nil {
		return
	}

	unsub, err := DecodeUnsubscribeMessage(buf)
	if err != nil {
		log.Error("Decode UnSubscribe Message error: ", err)
		c.Close()
		return
	}
	topics := unsub.Topics()

	for _, t := range topics {
		var sub *subscription
		ok := false

		if sub, ok = c.subs[string(t)]; ok {
			go c.unsubscribe(sub)
		}

	}

	resp := message.NewUnsubackMessage()
	resp.SetPacketId(unsub.PacketId())

	err1 := c.writeMessage(resp)
	if err1 != nil {
		log.Error("send ubsuback error, ", err1)
		return
	}
	// //process ubsubscribe message
	if c.typ == CLIENT {
		b.BroadcastSubOrUnsubMessage(buf)
	}
}

func (c *client) unsubscribe(sub *subscription) {

	c.mu.Lock()
	delete(c.subs, string(sub.topic))
	c.mu.Unlock()

	if c.broker != nil {
		c.broker.sl.Remove(sub)
	}
}

func (c *client) ProcessPing(buf []byte) {
	_, err := DecodePingreqMessage(buf)
	if err != nil {
		log.Error("Decode PingRequest Message error: ", err)
		c.Close()
		return
	}

	pingRspMsg := message.NewPingrespMessage()
	err = c.writeMessage(pingRspMsg)
	if err != nil {
		log.Error("send PingResponse error, ", err)
		return
	}
}

func (c *client) Close() {
	b := c.broker
	subs := c.subs
	if b != nil {
		b.removeClient(c)
		for _, sub := range subs {
			err := b.sl.Remove(sub)
			if err != nil {
				log.Error("closed client but remove sublist error, ", err)
			}
		}
		if c.info.willMsg != nil {
			b.ProcessPublishMessage(c.info.willMsg)
		}
	}
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func WriteBuffer(conn net.Conn, buf []byte) error {
	if conn == nil {
		return errors.New("conn is nul")
	}
	_, err := conn.Write(buf)
	return err
}
func (c *client) writeBuffer(buf []byte) error {
	c.mu.Lock()
	err := WriteBuffer(c.conn, buf)
	c.mu.Unlock()
	return err
}

func (c *client) writeMessage(msg message.Message) error {
	buf, err := EncodeMessage(msg)
	if err != nil {
		return err
	}
	return c.writeBuffer(buf)
}
