/* Copyright (c) 2018, joy.zhou <chowyu08@gmail.com>
 */
package broker

import (
	"strings"

	"github.com/fhmq/hmq/plugins/authhttp"
)

const (
	PUB = 1
	SUB = 2
)

func (c *client) CheckTopicAuth(typ int, topic string) bool {
	if c.typ != CLIENT || !c.broker.pluginAuthHTTP {
		return true
	}

	if strings.HasPrefix(topic, "$SYS/broker/connection/clients/") {
		return true
	}

	if strings.HasPrefix(topic, "$queue/") {
		topic = strings.TrimPrefix(topic, "$queue/")
	}

	access := "sub"
	switch typ {
	case 1:
		access = "2"
	case 2:
		access = "1"
	}
	username := string(c.info.username)
	return authhttp.CheckACL(username, access, topic)

}

func (b *Broker) CheckConnectAuth(clientID, username, password string) bool {
	if b.pluginAuthHTTP {
		if clientID == "" || username == "" {
			return false
		}
		return authhttp.CheckAuth(clientID, username, password)
	}

	return false

}
