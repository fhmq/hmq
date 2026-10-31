package auth

import (
	authfile "github.com/fhmq/hmq/v4/plugins/auth/authfile"
	"github.com/fhmq/hmq/v4/plugins/auth/authhttp"
)

const (
	AuthHTTP = "authhttp"
	AuthFile = "authfile"
)

type Auth interface {
	CheckACL(action, clientID, username, ip, topic string) bool
	CheckConnect(clientID, username, password string) bool
}

func NewAuth(name string) Auth {
	switch name {
	case AuthHTTP:
		return authhttp.Init()
	case AuthFile:
		return authfile.Init()
	default:
		return &mockAuth{}
	}
}
