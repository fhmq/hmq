/* Copyright (c) 2018, joy.zhou <chowyu08@gmail.com>
 */
package broker

import (
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
	access := "sub"
	switch typ {
	case 1:
		access = "pub"
	case 2:
		access = "sub"
	}
	username := string(c.info.username)
	return authhttp.CheckACL(username, access, topic)

}
