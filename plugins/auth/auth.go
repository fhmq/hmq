package auth

import (
	"github.com/fhmq/rhmq/plugins/auth/authhttp"
)

const (
	AuthHTTP = "authhttp"
)

type Auth interface {
	CheckACL(action, username, topic string) bool
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
