package broker

import "net"

type client struct {
	broker   Broker
	conn     net.Conn
	clientID []byte
	username []byte
	password []byte
}

func init() {

}
