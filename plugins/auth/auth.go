package auth

import (
	"github.com/fhmq/hmq/plugins/auth/authhttp"
)

const (
	AuthHTTP = "authhttp"
)

type Auth interface {
	CheckACL(action, clientID, username, ip, topic string) bool
	CheckConnect(clientID, username, password string) bool
}

func NewAuth(name string) Auth {
	switch name {
	case AuthHTTP:
		return authhttp.Init()
	default:
		return &mockAuth{}
	}
}
