package broker

import (
	"strings"
)

const (
	SUB = "1"
	PUB = "2"
)

func (broker *Broker) CheckTopicAuth(action, clientID, username, ip, topic string) bool {
	if broker.auth != nil {
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

		return broker.auth.CheckACL(action, clientID, username, ip, topic)
	}

	return true

}

func (broker *Broker) CheckConnectAuth(clientID, username, password string) bool {
	if broker.auth != nil {
		return broker.auth.CheckConnect(clientID, username, password)
	}

	return true

}
