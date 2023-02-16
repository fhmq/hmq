package broker

import (
	"github.com/fhmq/hmq/plugins/bridge"
	"go.uber.org/zap"
)

func (b *Broker) Publish(e *bridge.Elements) bool {
	if b.bridgeMQ != nil {
		cost, err := b.bridgeMQ.Publish(e)
		if err != nil {
			log.Error("send message to mq error.", zap.Error(err))
		}
		return cost
	}
	return false
}
