package broker

import (
	"fmt"
	"time"

	simplejson "github.com/bitly/go-simplejson"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"go.uber.org/zap"
)

func (c *client) SendInfo() {
	if c.status == Disconnected {
		return
	}
	url := c.info.localIP + ":" + c.broker.config.Cluster.Port

	infoMsg := NewInfo(c.broker.id, url, false)
	err := c.WriterPacket(infoMsg)
	if err != nil {
		log.Error("send info message error, ", zap.Error(err))
		return
	}
}

func (c *client) StartPing() {
	timeTicker := time.NewTicker(time.Second * 50)
	ping := packets.NewControlPacket(packets.Pingreq).(*packets.PingreqPacket)
	for {
		select {
		case <-timeTicker.C:
			err := c.WriterPacket(ping)
			if err != nil {
				log.Error("ping error: ", zap.Error(err))
				c.Close()
			}
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *client) SendConnect() {

	if c.status != Connected {
		return
	}
	m := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)

	m.CleanSession = true
	m.ClientIdentifier = c.info.clientID
	m.Keepalive = uint16(60)
	err := c.WriterPacket(m)
	if err != nil {
		log.Error("send connect message error, ", zap.Error(err))
		return
	}
	log.Info("send connect success")
}

func NewInfo(sid, url string, isforword bool) *packets.PublishPacket {
	pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	pub.Qos = 0
	pub.TopicName = BrokerInfoTopic
	pub.Retain = false
	info := fmt.Sprintf(`{"brokerID":"%s","brokerUrl":"%s"}`, sid, url)
	// log.Info("new info", string(info))
	pub.Payload = []byte(info)
	return pub
}

func (c *client) ProcessInfo(packet *packets.PublishPacket) {
	nc := c.conn
	b := c.broker
	if nc == nil {
		return
	}

	log.Info("recv remoteInfo: ", zap.String("payload", string(packet.Payload)))

	js, err := simplejson.NewJson(packet.Payload)
	if err != nil {
		log.Warn("parse info message err", zap.Error(err))
		return
	}

	routes, err := js.Get("data").Map()
	if routes == nil {
		log.Error("receive info message error, ", zap.Error(err))
		return
	}

	b.nodes = routes

	b.mu.Lock()
	for rid, rurl := range routes {
		if rid == b.id {
			continue
		}

		url, ok := rurl.(string)
		if ok {
			exist := b.CheckRemoteExist(rid, url)
			if !exist {
				b.connectRouter(rid, url)
			}
		}

	}
	b.mu.Unlock()
}
