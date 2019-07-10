/* Copyright (c) 2018, joy.zhou <chowyu08@gmail.com>
 */
package broker

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/fhmq/hmq/plugins"
	"github.com/fhmq/hmq/plugins/kafka"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/lib/sessions"
	"github.com/fhmq/hmq/lib/topics"
	"go.uber.org/zap"
)

const (
	// special pub topic for cluster info BrokerInfoTopic
	BrokerInfoTopic = "broker000100101info"
	// CLIENT is an end user.
	CLIENT = 0
	// ROUTER is another router in the cluster.
	ROUTER = 1
	//REMOTE is the router connect to other cluster
	REMOTE  = 2
	CLUSTER = 3
)
const (
	Connected    = 1
	Disconnected = 2
)

type client struct {
	typ        int
	mu         sync.Mutex
	broker     *Broker
	conn       net.Conn
	info       info
	route      route
	status     int
	ctx        context.Context
	cancelFunc context.CancelFunc
	session    *sessions.Session
	subMap     map[string]*subscription
	topicsMgr  *topics.Manager
	subs       []interface{}
	qoss       []byte
	rmsgs      []*packets.PublishPacket
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
	c.status = Connected
	c.info.localIP = strings.Split(c.conn.LocalAddr().String(), ":")[0]
	c.info.remoteIP = strings.Split(c.conn.RemoteAddr().String(), ":")[0]
	c.ctx, c.cancelFunc = context.WithCancel(context.Background())
	c.subMap = make(map[string]*subscription)
	c.topicsMgr = c.broker.topicsMgr
}

func (c *client) readLoop() {
	nc := c.conn
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
			if err := nc.SetReadDeadline(time.Now().Add(timeOut)); err != nil {
				log.Error("set read timeout error: ", zap.Error(err), zap.String("ClientID", c.info.clientID))
				return
			}

			packet, err := packets.ReadPacket(nc)
			if err != nil {
				log.Error("read packet error: ", zap.Error(err), zap.String("ClientID", c.info.clientID))
				msg := &Message{client: c, packet: DisconnectdPacket}
				b.SubmitWork(c.info.clientID, msg)
				return
			}

			msg := &Message{
				client: c,
				packet: packet,
			}
			b.SubmitWork(c.info.clientID, msg)
		}
	}

}

func ProcessMessage(msg *Message) {
	c := msg.client
	ca := msg.packet
	if ca == nil {
		return
	}
	log.Debug("Recv message:", zap.String("message type", reflect.TypeOf(msg.packet).String()[9:]), zap.String("ClientID", c.info.clientID))
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
		log.Info("Recv Unknow message.......", zap.String("ClientID", c.info.clientID))
	}
}

func (c *client) ProcessPublish(packet *packets.PublishPacket) {
	if c.status == Disconnected {
		return
	}

	topic := packet.TopicName
	if topic == BrokerInfoTopic && c.typ == CLUSTER {
		c.ProcessInfo(packet)
		return
	}

	if !c.CheckTopicAuth(PUB, topic) {
		log.Error("Pub Topics Auth failed, ", zap.String("topic", topic), zap.String("ClientID", c.info.clientID))
		return
	}

	if c.broker.pluginKafka && c.typ == CLIENT {
		kafka.Publish(&plugins.Elements{
			ClientID:  c.info.clientID,
			Username:  c.info.username,
			Action:    plugins.Publish,
			Timestamp: time.Now().Unix(),
			Payload:   string(packet.Payload),
			Topic:     topic,
		})
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

func (c *client) ProcessPublishMessage(packet *packets.PublishPacket) {

	b := c.broker
	if b == nil {
		return
	}
	typ := c.typ

	if packet.Retain {
		if err := c.topicsMgr.Retain(packet); err != nil {
			log.Error("Error retaining message: ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		}
	}

	c.mu.Lock()
	err := c.topicsMgr.Subscribers([]byte(packet.TopicName), packet.Qos, &c.subs, &c.qoss)
	c.mu.Unlock()
	if err != nil {
		log.Error("Error retrieving subscribers list: ", zap.String("ClientID", c.info.clientID))
		return
	}

	// log.Info("psubs num: ", len(r.psubs))
	if len(c.subs) == 0 {
		return
	}

	for _, sub := range c.subs {
		s, ok := sub.(*subscription)
		if ok {
			if s.client.typ == ROUTER {
				if typ != CLIENT {
					continue
				}
			}
			err := s.client.WriterPacket(packet)
			if err != nil {
				log.Error("process message for psub error,  ", zap.Error(err), zap.String("ClientID", c.info.clientID))
			}
		}

	}

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
			log.Error("Sub topic Auth failed: ", zap.String("topic", topic), zap.String("ClientID", c.info.clientID))
			retcodes = append(retcodes, QosFailure)
			continue
		}

		if c.broker.pluginKafka && c.typ == CLIENT {
			kafka.Publish(&plugins.Elements{
				ClientID:  c.info.clientID,
				Username:  c.info.username,
				Action:    plugins.Subscribe,
				Timestamp: time.Now().Unix(),
				Topic:     topic,
			})
		}

		sub := &subscription{
			topic:  t,
			qos:    qoss[i],
			client: c,
		}

		rqos, err := c.topicsMgr.Subscribe([]byte(topic), qoss[i], sub)
		if err != nil {
			return
		}

		c.subMap[topic] = sub
		c.session.AddTopic(topic, qoss[i])
		retcodes = append(retcodes, rqos)
		c.topicsMgr.Retained([]byte(topic), &c.rmsgs)

	}

	suback.ReturnCodes = retcodes

	err := c.WriterPacket(suback)
	if err != nil {
		log.Error("send suback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}
	//broadcast subscribe message
	if c.typ == CLIENT {
		go b.BroadcastSubOrUnsubMessage(packet)
	}

	//process retain message
	for _, rm := range c.rmsgs {
		if err := c.WriterPacket(rm); err != nil {
			log.Error("Error publishing retained message:", zap.Any("err", err), zap.String("ClientID", c.info.clientID))
		} else {
			log.Info("process retain  message: ", zap.Any("packet", packet), zap.String("ClientID", c.info.clientID))
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
	topics := packet.Topics

	for _, topic := range topics {
		t := []byte(topic)
		sub, exist := c.subMap[topic]
		if exist {
			c.topicsMgr.Unsubscribe(t, sub)
			c.session.RemoveTopic(topic)
			delete(c.subMap, topic)
		}

		if c.broker.pluginKafka && c.typ == CLIENT {
			kafka.Publish(&plugins.Elements{
				ClientID:  c.info.clientID,
				Username:  c.info.username,
				Action:    plugins.Unsubscribe,
				Timestamp: time.Now().Unix(),
				Topic:     topic,
			})
		}
	}

	unsuback := packets.NewControlPacket(packets.Unsuback).(*packets.UnsubackPacket)
	unsuback.MessageID = packet.MessageID

	err := c.WriterPacket(unsuback)
	if err != nil {
		log.Error("send unsuback error, ", zap.Error(err), zap.String("ClientID", c.info.clientID))
		return
	}
	// //process ubsubscribe message
	if c.typ == CLIENT {
		b.BroadcastSubOrUnsubMessage(packet)
	}
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
	if c.status == Disconnected {
		return
	}

	c.cancelFunc()

	c.status = Disconnected
	//wait for message complete
	// time.Sleep(1 * time.Second)
	// c.status = Disconnected

	if c.broker.pluginKafka && c.typ == CLIENT {
		kafka.Publish(&plugins.Elements{
			ClientID:  c.info.clientID,
			Username:  c.info.username,
			Action:    plugins.Disconnect,
			Timestamp: time.Now().Unix(),
		})
	}

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}

	b := c.broker
	subs := c.subMap
	if b != nil {
		b.removeClient(c)

		if c.typ == CLIENT {
			b.BroadcastUnSubscribe(subs)
			//offline notification
			b.OnlineOfflineNotification(c.info.clientID, false)
		}

		if c.info.willMsg != nil {
			b.PublishMessage(c.info.willMsg)
		}

		if c.typ == CLUSTER {
			b.ConnectToDiscovery()
		}

		//do reconnect
		if c.typ == REMOTE {
			go b.connectRouter(c.route.remoteID, c.route.remoteUrl)
		}
	}
}

func (c *client) WriterPacket(packet packets.ControlPacket) error {
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
	err := packet.Write(c.conn)
	c.mu.Unlock()
	return err
}
