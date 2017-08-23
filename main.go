package main

import (
	"fhmq/broker"
	"os"
	"os/signal"

	log "github.com/cihub/seelog"
)

func main() {
	broker := broker.NewBroker()
	broker.StartListening()

	s := waitForSignal()
	log.Infof("signal got: %v ,broker closed.", s)
}

func waitForSignal() os.Signal {
	signalChan := make(chan os.Signal, 1)
	defer close(signalChan)
	signal.Notify(signalChan, os.Kill, os.Interrupt)
	s := <-signalChan
	signal.Stop(signalChan)
	return s
}
