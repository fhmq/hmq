package main

import (
	"os"
	"os/signal"
	"runtime"

	"github.com/fhmq/hmq/broker"

	log "github.com/cihub/seelog"
)

func init() {
	testConfig := `
<seelog type="sync">
	<outputs formatid="main">
		<console/>
	</outputs>
	<formats>
		<format id="main" format="Time:%Date %Time%tfile:%File%tlevel:%LEVEL%t%Msg%n"/>
	</formats>
</seelog>`

	logger, err := log.LoggerFromConfigAsBytes([]byte(testConfig))
	if err != nil {
		panic(err)
	}
	log.ReplaceLogger(logger)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	config, er := broker.LoadConfig()
	if er != nil {
		log.Error("Load Config file error: ", er)
		return
	}

	b, err := broker.NewBroker(config)
	if err != nil {
		log.Error("New Broker error: ", er)
		return
	}
	b.Start()

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
