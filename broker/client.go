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
	woker    Worker
}

type info struct {
	clientID  []byte
	username  []byte
	password  []byte
	keepalive uint16
	willMsg   *message.PublishMessage
}

func (c *client) init() {
	c.localIP = strings.Split(c.conn.LocalAddr().String(), ":")[0]
	c.remoteIP = strings.Split(c.conn.RemoteAddr().String(), ":")[0]
}

func (c *client) readLoop(idx int) {
	nc := c.conn
	msgPool := MSGPool[idx%MessagePoolNum].GetPool()
	if nc == nil || msgPool == nil {
		return
	}
	for {
		buf, err := ReadPacket(nc)
		if err != nil {
			log.Error("read packet error: ", err)
			return
		}
		msg.client = c
		msg.buf = buf
		msgPool.Push(msg)
	}
	msgPool.Reduce()
}
