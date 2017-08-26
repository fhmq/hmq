package broker

import (
	"hmq/lib/acl"
	"strings"
)

const (
	PUB = 1
	SUB = 2
)

func (c *client) CheckTopicAuth(typ int, topic string) bool {
	if !c.broker.config.Acl {
		return true
	}
	if strings.HasPrefix(topic, "$queue/") {
		topic = string([]byte(topic)[7:])
		if topic == "" {
			return false
		}
	}
	ip := c.info.remoteIP
	username := string(c.info.username)
	clientid := string(c.info.clientID)
	aclInfo := c.broker.AclConfig
	return acl.CheckTopicAuth(aclInfo, typ, ip, username, clientid, topic)

}
