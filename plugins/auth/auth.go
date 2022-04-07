package auth

import (
	authfile "github.com/fhmq/hmq/plugins/auth/authfile"
	"github.com/fhmq/hmq/plugins/auth/authhttp"
)

const (
	AuthHTTP = "authhttp"
	AuthFile = "authfile"
)

type Auth interface {
	CheckACL(action, clientID, username, ip, topic string) bool
	CheckConnect(clientID, username, password string) bool
}

func NewAuth(name string, confFile string) Auth {
	switch name {
	case AuthHTTP:
		return authhttp.Init(confFile)
	case AuthFile:
		return authfile.Init(confFile)
	default:
		return &mockAuth{}
	}
}
