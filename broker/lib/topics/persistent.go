package topics

import (
	"fmt"
	"sync"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/v3/broker/lib/topics/retained"
	"github.com/fhmq/hmq/v3/broker/storage"
)

// NewPersistentManager creates an independent manager with durable retained
// messages and in-memory subscriptions. The caller owns it and must close it.
func NewPersistentManager(path string) (*Manager, error) {
	store, err := retained.Open(path)
	if err != nil {
		return nil, err
	}
	p := &persistentTopics{memTopics: NewMemProvider(), store: store}
	for _, m := range store.All() {
		msg := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		msg.TopicName, msg.Qos, msg.Payload = m.Topic, m.QoS, m.Payload
		if err := p.memTopics.Retain(msg); err != nil {
			store.Close()
			return nil, err
		}
	}
	return &Manager{p: p}, nil
}

type persistentTopics struct {
	*memTopics
	mu     sync.Mutex
	store  *retained.Store
	err    error
	closed bool
}

func (p *persistentTopics) Retain(msg *packets.PublishPacket) error {
	if msg == nil {
		return fmt.Errorf("message cannot be nil")
	}
	if err := validateTopic([]byte(msg.TopicName), false); err != nil {
		return err
	}
	if !ValidQos(msg.Qos) {
		return fmt.Errorf("invalid QoS %d", msg.Qos)
	}
	canonical := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	canonical.TopicName, canonical.Qos = msg.TopicName, msg.Qos
	canonical.Payload = append([]byte(nil), msg.Payload...)
	msg = canonical
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return storage.ErrClosed
	}
	if p.err != nil {
		return p.err
	}
	var err error
	if len(msg.Payload) == 0 {
		err = p.store.Delete(msg.TopicName)
	} else {
		err = p.store.Put(retained.Message{Topic: msg.TopicName, QoS: msg.Qos, Payload: msg.Payload})
	}
	if err != nil {
		p.err = err
		return err
	}
	if err = p.memTopics.Retain(msg); err != nil {
		p.err = err
		return err
	}
	return nil
}

func (p *persistentTopics) Retained(topic []byte, msgs *[]*packets.PublishPacket) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return storage.ErrClosed
	}
	if p.err != nil {
		return p.err
	}
	return p.memTopics.Retained(topic, msgs)
}

func (p *persistentTopics) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return storage.ErrClosed
	}
	p.closed = true
	err := p.store.Close()
	p.memTopics.Close()
	return err
}
