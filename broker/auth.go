/* Copyright (c) 2018, joy.zhou <chowyu08@gmail.com>
 */
package broker

import (
	"strings"

	"github.com/fhmq/hmq/plugins/authhttp"
)

const (
	SUB = "1"
	PUB = "2"
)

func (b *Broker) CheckTopicAuth(action, username, topic string) bool {
	if !b.pluginAuthHTTP {
		return true
	}

	if strings.HasPrefix(topic, "$SYS/broker/connection/clients/") {
		return true
	}

	if strings.HasPrefix(topic, "$queue/") {
		topic = strings.TrimPrefix(topic, "$queue/")
	}

	return authhttp.CheckACL(username, action, topic)

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
