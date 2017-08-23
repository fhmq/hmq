package broker

import (
	"fhmq/lib/message"
	"net"
	"strings"

	"github.com/prometheus/common/log"
)

type client struct {
	broker   *Broker
	conn     net.Conn
	info     info
	localIP  string
	remoteIP string
	msgPool  *MessagePool
	woker    Worker
}

type info struct {
	clientID  []byte
	username  []byte
	password  []byte
	keepalive uint16
	willMsg   *message.PublishMessage
}

type Worker struct {
	WorkerPool chan chan Message
	MsgChannel chan Message
	quit       chan bool
}

func (c *client) init() {
	c.localIP = strings.Split(c.conn.LocalAddr().String(), ":")[0]
	c.remoteIP = strings.Split(c.conn.RemoteAddr().String(), ":")[0]
}

func (c *client) readLoop() {
	nc := c.conn
	msgPool := c.msgPool
	if nc == nil || msgPool == nil {
		return
	}
	cid := c.info.clientID
	for {
		msg := msgPool.Pop()
		buf, err := ReadPacket(nc)
		if err != nil {
			log.Error("read packet error: ", err)
			return
		}
		msg.user = cid
		msg.buf = buf

	}
}
