package broker

import (
	"strings"
)

const (
	SUB = "1"
	PUB = "2"
)

func (b *Broker) CheckTopicAuth(action, clientID, username, ip, topic string) bool {
	if b.auth != nil {
		if strings.HasPrefix(topic, "$SYS/broker/connection/clients/") {
			return true
		}

		if strings.HasPrefix(topic, "$share/") && action == SUB {
			substr := groupCompile.FindStringSubmatch(topic)
			if len(substr) != 3 {
				return false
			}
			topic = substr[2]
		}

		return b.auth.CheckACL(action, clientID, username, ip, topic)
	}

	return true

}

func (b *Broker) CheckConnectAuth(clientID, username, password string) bool {
	if b.auth != nil {
		return b.auth.CheckConnect(clientID, username, password)
	}

	return true

}
