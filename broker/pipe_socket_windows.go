package broker

import (
	"fmt"
	"github.com/natefinch/npipe"
	"go.uber.org/zap"
	"net"
	"time"
)

// StartPipeSocketListening We use the open source npipe library to support pipe communication in windows
func (b *Broker) StartPipeSocketListening(pipeName string, usePipe bool) {
	var err error
	var ln *npipe.PipeListener

	for {
		if usePipe {
			fmt.Println(pipeName)
			ln, err = npipe.Listen(pipeName)
			log.Info("Start Listening client on ", zap.String("pipeName", pipeName))
		}
		if err == nil {
			break // successfully listening
		}
		log.Error("Error listening on ", zap.Error(err))
		time.Sleep(1 * time.Second)
	}

	tmpDelay := 10 * ACCEPT_MIN_SLEEP

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Error(
					"Temporary Client Accept Error(%v), sleeping %dms",
					zap.Error(ne),
					zap.Duration("sleeping", tmpDelay/time.Millisecond),
				)

				time.Sleep(tmpDelay)
				tmpDelay *= 2
				if tmpDelay > ACCEPT_MAX_SLEEP {
					tmpDelay = ACCEPT_MAX_SLEEP
				}
			} else {
				log.Error("Accept error", zap.Error(err))
			}
			continue
		}

		tmpDelay = ACCEPT_MIN_SLEEP
		go func() {
			err := b.handleConnection(CLIENT, conn)
			fmt.Println("handleConnection,", err)
			if err != nil {
				conn.Close()
			}
		}()
	}
}
