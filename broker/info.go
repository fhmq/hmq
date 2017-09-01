package broker

import (
	"fmt"
	"hmq/packets"
	"time"

	simplejson "github.com/bitly/go-simplejson"
	log "github.com/cihub/seelog"
)

func (c *client) SendInfo() {
	url := c.info.localIP + ":" + c.broker.config.Cluster.Port

	infoMsg := NewInfo(c.broker.id, url, false)
	err := c.WriterPacket(infoMsg)
	if err != nil {
		log.Error("send info message error, ", err)
		return
	}
	// log.Info("send info success")
}

func (c *client) StartPing() {
	timeTicker := time.NewTicker(time.Second * 30)
	ping := packets.NewControlPacket(packets.Pingreq).(*packets.PingreqPacket)
	for {
		select {
		case <-timeTicker.C:
			err := c.WriterPacket(ping)
			if err != nil {
				log.Error("ping error: ", err)
			}
		}
	}
}

func (c *client) SendConnect() {

	m := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)

	m.CleanSession = true
	m.ClientIdentifier = c.info.clientID
	m.Keepalive = uint16(60)
	err := c.WriterPacket(m)
	if err != nil {
		log.Error("send connect message error, ", err)
		return
	}
	// log.Info("send connet success")
}

func NewInfo(sid, url string, isforword bool) *packets.PublishPacket {
	pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	pub.Qos = 0
	pub.TopicName = BrokerInfoTopic
	pub.Retain = false
	info := fmt.Sprintf(`{"remoteID":"%s","url":"%s","isForward":%t}`, sid, url, isforword)
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

	log.Info("recv remoteInfo: ", string(packet.Payload))

	js, e := simplejson.NewJson(packet.Payload)
	if e != nil {
		log.Warn("parse info message err", e)
		return
	}

	rid := js.Get("remoteID").MustString()
	rurl := js.Get("url").MustString()
	isForward := js.Get("isForward").MustBool()

	if rid == "" {
		log.Error("receive info message error with remoteID is null")
		return
	}

	if rid == b.id {
		if !isForward {
			c.Close() //close connet self
		}
		return
	}

	exist := b.CheckRemoteExist(rid, rurl)
	if !exist {
		go b.connectRouter(rurl, rid)
	}
	// log.Info("isforword: ", isForward)
	if !isForward {
		route := &route{
			remoteUrl: rurl,
			remoteID:  rid,
		}
		c.route = route

		go b.SendLocalSubsToRouter(c)
		// log.Info("BroadcastInfoMessage starting... ")
		infoMsg := NewInfo(rid, rurl, true)
		b.BroadcastInfoMessage(rid, infoMsg)
	}

	return
}
