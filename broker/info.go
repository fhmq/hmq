package broker

import (
	"fmt"
	"hmq/lib/message"
	"time"

	simplejson "github.com/bitly/go-simplejson"
	log "github.com/cihub/seelog"
)

func (c *client) SendInfo() {
	url := c.info.localIP + ":" + c.broker.config.Cluster.Port

	infoMsg := NewInfo(c.broker.id, url, false)
	err := c.writeMessage(infoMsg)
	if err != nil {
		log.Error("send info message error, ", err)
		return
	}
	// log.Info("send info success")
}

func (c *client) StartPing() {
	timeTicker := time.NewTicker(time.Second * 30)
	ping := message.NewPingreqMessage()
	for {
		select {
		case <-timeTicker.C:
			err := c.writeMessage(ping)
			if err != nil {
				log.Error("ping error: ", err)
			}
		}
	}
}

func (c *client) SendConnect() {

	clientID := c.info.clientID
	connMsg := message.NewConnectMessage()
	connMsg.SetClientId([]byte(clientID))
	connMsg.SetVersion(0x04)
	err := c.writeMessage(connMsg)
	if err != nil {
		log.Error("send connect message error, ", err)
		return
	}
	// log.Info("send connet success")
}

func NewInfo(sid, url string, isforword bool) *message.PublishMessage {
	infoMsg := message.NewPublishMessage()
	infoMsg.SetTopic([]byte(BrokerInfoTopic))
	info := fmt.Sprintf(`{"remoteID":"%s","url":"%s","isForward":%t}`, sid, url, isforword)
	// log.Info("new info", string(info))
	infoMsg.SetPayload([]byte(info))
	infoMsg.SetQoS(0)
	infoMsg.SetRetain(false)
	return infoMsg
}

func (c *client) ProcessInfo(msg *message.PublishMessage) {
	nc := c.conn
	b := c.broker
	if nc == nil {
		return
	}

	log.Info("recv remoteInfo: ", string(msg.Payload()))

	js, e := simplejson.NewJson(msg.Payload())
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
