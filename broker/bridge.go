package broker

import (
	"hmq/plugins/bridge"

	"go.uber.org/zap"
)

func (broker *Broker) Publish(e *bridge.Elements) bool {
	if broker.bridgeMQ != nil {
		cost, err := broker.bridgeMQ.Publish(e)
		if err != nil {
			log.Error("send message to mq error.", zap.Error(err))
		}
		return cost
	}
	return false
}
