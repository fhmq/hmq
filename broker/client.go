package broker

import (
	"fhmq/lib/message"
	"net"
	"strings"
)

type client struct {
	broker   Broker
	conn     net.Conn
	info     info
	localIP  string
	remoteIP string
}

type info struct {
	clientID []byte
	username []byte
	password []byte
	keeplive uint16
	willMsg  *message.PublishMessage
}

func (c *client) init() {
	c.localIP = strings.Split(c.conn.LocalAddr().String(), ":")[0]
	c.remoteIP = strings.Split(c.conn.RemoteAddr().String(), ":")[0]
}
