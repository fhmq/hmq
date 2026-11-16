package broker

import (
	"golang.org/x/net/websocket"
	"net"
)

func connectionIP(c net.Conn) string {
	address := c.RemoteAddr().String()
	if ws, ok := c.(*websocket.Conn); ok {
		address = ws.Request().RemoteAddr
	}
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return ""
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	return ""
}
