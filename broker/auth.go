/* Copyright (c) 2018, joy.zhou <chowyu08@gmail.com>
 */
package broker

import (
	"strings"
	"crypto/tls"
)

const (
	SUB = "1"
	PUB = "2"
)

func (b *Broker) CheckTopicAuth(action string, client *client, topic string) bool {
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

		// Replace username by CN if tls connection
		username := string(client.info.username)
		tlscon, ok := client.conn.(*tls.Conn)
		if ok {
			state := tlscon.ConnectionState()
			for _, certificate := range state.PeerCertificates {
				username=certificate.Subject.CommonName
			}
		}

		return b.auth.CheckACL(action, username, topic)
	}

	return true

}

func (b *Broker) CheckConnectAuth(clientID, username, password string) bool {
	if b.auth != nil {
		return b.auth.CheckConnect(clientID, username, password)
	}

	return true

}
