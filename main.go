package main

import (
	"hmq/broker"
	"os"
	"os/signal"
	"runtime"

	log "github.com/cihub/seelog"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	runtime.GC()
	config, er := broker.LoadConfig()
	if er != nil {
		log.Error("Load Config file error: ", er)
		return
	}

	broker, err := broker.NewBroker(config)
	if err != nil {
		log.Error("New Broker error: ", er)
		return
	}
	broker.Start()

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
