package broker

import (
	"github.com/fhmq/hmq/plugins/bridge"
	"go.uber.org/zap"
)

func (b *Broker) Publish(e *bridge.Elements) {
	if b.BridgeMQ != nil {
		err := b.BridgeMQ.Publish(e)
		if err != nil {
			log.Error("send message to mq error.", zap.Error(err))
		}
	}
}
