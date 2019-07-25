package broker

import (
	"github.com/fhmq/hmq/plugins"
	"github.com/fhmq/hmq/plugins/kafka"
)

func (b *Broker) Publish(e *plugins.Elements) {
	if b.pluginKafka {
		kafka.Publish(e)
	}
}
