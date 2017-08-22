package broker

import (
	"net"
	"time"

	log "github.com/cihub/seelog"
)

type Broker struct {
}

func NewBroker() *Broker {
	return &Broker{}
}
func (b *Broker) StartListening() {
	l, e := net.Listen("tcp", "0.0.0.0:1883")
	if e != nil {
		log.Error("Error listening on ", e)
		return
	}
	tmpDelay := 10 * ACCEPT_MIN_SLEEP
	for {
		conn, err := l.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Error("Temporary Client Accept Error(%v), sleeping %dms",
					ne, tmpDelay/time.Millisecond)
				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else {
				log.Error("Accept error: %v", err)
			}
			continue
		}
		tmpDelay = ACCEPT_MIN_SLEEP
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {

	//process connect packet
	connMsg, err := ReadPacket(conn)
	if err != nil {
		log.Error("read connect packet error: ", err)
		return
	}

}
