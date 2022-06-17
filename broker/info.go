package broker

import (
	"context"
	"fmt"
	"time"

	"hmq/broker/packets"

	simplejson "github.com/bitly/go-simplejson"
	"go.uber.org/zap"
)

// SendInfo sends information of current client to a broker in cluster
func (c *client) SendInfo(ctx context.Context, cancel context.CancelFunc) {
	if c.status == Disconnected {
		return
	}
	url := c.info.localIP + ":" + c.broker.config.Cluster.Port

	infoMsg := NewInfo(c.broker.id, url)
	err := c.WriterPacket(infoMsg, ctx, cancel)
	if err != nil {
		log.Error("send info message error, ", zap.Error(err))
		return
	}
}

func (c *client) StartPing(ctx context.Context, cancel context.CancelFunc) {
	timeTicker := time.NewTicker(time.Second * 50)
	ping := packets.NewControlPacket(packets.Pingreq).(*packets.PingreqPacket)
	for {
		select {
		case <-timeTicker.C:
			err := c.WriterPacket(ping, ctx, cancel)
			if err != nil {
				log.Error("ping error: ", zap.Error(err))
				c.Close(ctx, cancel)
			}
		case <-ctx.Done():
			return
		}
	}
}

// SendConnect sends a connect packet
func (c *client) SendConnect(ctx context.Context, cancel context.CancelFunc) {

	if c.status != Connected {
		return
	}
	m := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	m.ProtocolName = "MQIsdp"
	m.ProtocolVersion = 3

	m.CleanSession = true
	m.ClientIdentifier = c.info.clientID
	m.Keepalive = uint16(60)
	err := c.WriterPacket(m, ctx, cancel)
	if err != nil {
		log.Error("send connect message error, ", zap.Error(err))
		return
	}
	log.Info("send connect success")
}

func NewInfo(sid, url string) *packets.PublishPacket {
	pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	pub.Qos = 0
	pub.TopicName = InfoTopic
	pub.Retain = false
	info := fmt.Sprintf(`{"brokerID":"%s","brokerUrl":"%s"}`, sid, url)
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
