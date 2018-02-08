/* Copyright (c) 2018, joy.zhou <chowyu08@gmail.com>

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.
*/
package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"

	"github.com/fhmq/hmq/broker"
	"github.com/fhmq/hmq/logger"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	config, err := broker.ConfigureConfig(os.Args[1:])
	if err != nil {
		fmt.Println("configure broker config error: ", err)
		return
	}
	logger.InitLogger(config.Debug)
	b, err := broker.NewBroker(config, logger.Get())
	if err != nil {
		fmt.Println("New Broker error: ", err)
		return
	}
	b.Start()

	s := waitForSignal()
	fmt.Println("signal received, broker closed.", s)
}

func waitForSignal() os.Signal {
	signalChan := make(chan os.Signal, 1)
	defer close(signalChan)
	signal.Notify(signalChan, os.Kill, os.Interrupt)
	s := <-signalChan
	signal.Stop(signalChan)
	return s
}
