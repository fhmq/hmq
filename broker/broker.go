package broker

import (
	"fhmq/lib/message"
	"net"
	"time"

	log "github.com/cihub/seelog"
)

type Broker struct {
	sl     *Sublist
	rl     *RetainList
	queues map[string]int
}

func NewBroker() *Broker {
	return &Broker{
		sl:     NewSublist(),
		rl:     NewRetainList(),
		queues: make(map[string]int),
	}
}
func (b *Broker) StartListening() {
	l, e := net.Listen("tcp", "0.0.0.0:1883")
	if e != nil {
		log.Error("Error listening on ", e)
		return
	}
	tmpDelay := 10 * ACCEPT_MIN_SLEEP
	num := 0
	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Error("Temporary Client Accept Error(%v), sleeping %dms",
					ne, tmpDelay/time.Millisecond)
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else {
				log.Error("Accept error: %v", err)
			}
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP
		num += 1
		go b.handleConnection(conn, num)
	}
}

func (b *Broker) handleConnection(conn net.Conn, idx int) {
	//process connect packet
	buf, err := ReadPacket(conn)
	if err != nil {
		log.Error("read connect packet error: ", err)
		return
	}
	connMsg, err := DecodeConnectMessage(buf)
	if err != nil {
		log.Error(err)
		return
	}

	connack := message.NewConnackMessage()
	connack.SetReturnCode(message.ConnectionAccepted)
	ack, _ := EncodeMessage(connack)
	err1 := WriteBuffer(conn, ack)
	if err1 != nil {
		log.Error("send connack error, ", err1)
		return
	}

	willmsg := message.NewPublishMessage()
	if connMsg.WillFlag() {
		willmsg.SetQoS(connMsg.WillQos())
		willmsg.SetPayload(connMsg.WillMessage())
		willmsg.SetRetain(connMsg.WillRetain())
		willmsg.SetTopic(connMsg.WillTopic())
		willmsg.SetDup(false)
	} else {
		willmsg = nil
	}
	info := info{
		clientID:  connMsg.ClientId(),
		username:  connMsg.Username(),
		password:  connMsg.Password(),
		keepalive: connMsg.KeepAlive(),
		willMsg:   willmsg,
	}

	c := &client{
		broker: b,
		conn:   conn,
		info:   info,
	}
	c.init()
	c.readLoop(idx)
}
