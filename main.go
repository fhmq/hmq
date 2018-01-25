/* Copyright (c) 2018, joy.zhou <chowyu08@gmail.com>

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.
*/
package main

import (
	"os"
	"os/signal"
	"runtime"

	"github.com/fhmq/hmq/broker"
	"github.com/fhmq/hmq/logger"
	"go.uber.org/zap"
)

var (
	log = logger.Get().Named("Main")
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	config, err := broker.ConfigureConfig()
	if err != nil {
		log.Error("configure broker config error: ", zap.Error(err))
		return
	}

	b, err := broker.NewBroker(config)
	if err != nil {
		log.Error("New Broker error: ", zap.Error(err))
		return
	}
	b.Start()

	s := waitForSignal()
	log.Info("signal received, broker closed.", zap.Any("signal", s))
}

func waitForSignal() os.Signal {
	signalChan := make(chan os.Signal, 1)
	defer close(signalChan)
	signal.Notify(signalChan, os.Kill, os.Interrupt)
	s := <-signalChan
	signal.Stop(signalChan)
	return s
}
