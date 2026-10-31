package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/fhmq/hmq/v4/broker"
	"github.com/fhmq/hmq/v4/logger"
	"go.uber.org/zap"
)

var log = logger.Get()

func main() {
	if printVersion(os.Args[1:], os.Stdout) {
		return
	}
	config, err := broker.ConfigureConfig(os.Args[1:])
	if err != nil {
		log.Fatal("configure broker config error", zap.Error(err))
	}

	b, err := broker.NewBroker(config)
	if err != nil {
		log.Fatal("New Broker error: ", zap.Error(err))
	}
	b.Start()

	s := waitForSignal()
	b.Close()
	log.Info("signal received, broker closed.", zap.Any("signal", s))
}

func waitForSignal() os.Signal {
	signalChan := make(chan os.Signal, 1)
	defer close(signalChan)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	s := <-signalChan
	signal.Stop(signalChan)
	return s
}
