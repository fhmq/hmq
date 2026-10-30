package broker

import (
	"errors"
	"testing"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/v3/broker/lib/topics"
)

type failingRetainProvider struct {
	topics.TopicsProvider
	queried bool
}

func (p *failingRetainProvider) Retain(*packets.PublishPacket) error {
	return errors.New("retain unavailable")
}

func (p *failingRetainProvider) Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error {
	p.queried = true
	return nil
}

func TestClusterRetainFailureStillDelivers(t *testing.T) {
	p := &failingRetainProvider{TopicsProvider: topics.NewMemProvider()}
	const name = "cluster-retain-failure-test"
	topics.Register(name, p)
	t.Cleanup(func() { topics.Unregister(name) })
	mgr, err := topics.NewManager(name)
	if err != nil {
		t.Fatal(err)
	}
	b := &Broker{topicsMgr: mgr}
	b.receiveClusterMessage("peer", []byte(`{"Topic":"test","Payload":"eA==","QoS":0,"Retain":true}`))
	if !p.queried {
		t.Fatal("retain failure prevented live delivery")
	}
}
