package broker

import (
	"sync"
	"testing"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

func TestQoSDeliveryConcurrentDisconnect(t *testing.T) {
	for i := 0; i < 100; i++ {
		c := &client{status: Connected, inflight: make(map[uint16]*inflightElem)}
		sub := &subscription{client: c}
		p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		p.TopicName = "test"
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); publish(sub, p, 1) }()
		go func() { defer wg.Done(); c.Close() }()
		wg.Wait()
		publish(sub, p, 2)
		c.inflightMu.RLock()
		n := len(c.inflight)
		c.inflightMu.RUnlock()
		c.retryTimerLock.Lock()
		timer := c.retryTimer
		c.retryTimerLock.Unlock()
		if n != 0 || timer != nil {
			t.Fatalf("disconnected client retained %d inflight messages or a retry timer", n)
		}
	}
}
