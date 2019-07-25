package topics

import (
	"fmt"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	// MWC is the multi-level wildcard
	MWC = "#"

	// SWC is the single level wildcard
	SWC = "+"

	// SEP is the topic level separator
	SEP = "/"

	// SYS is the starting character of the system level topics
	SYS = "$"

	// Both wildcards
	_WC = "#+"
)

var (
	providers = make(map[string]TopicsProvider)
)

// TopicsProvider
type TopicsProvider interface {
	Subscribe(topic []byte, qos byte, subscriber interface{}) (byte, error)
	Unsubscribe(topic []byte, subscriber interface{}) error
	Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error
	Retain(msg *packets.PublishPacket) error
	Retained(topic []byte, msgs *[]*packets.PublishPacket) error
	Close() error
}

func Register(name string, provider TopicsProvider) {
	if provider == nil {
		panic("topics: Register provide is nil")
	}

	if _, dup := providers[name]; dup {
		panic("topics: Register called twice for provider " + name)
	}

	providers[name] = provider
}

func Unregister(name string) {
	delete(providers, name)
}

type Manager struct {
	p TopicsProvider
}

func NewManager(providerName string) (*Manager, error) {
	p, ok := providers[providerName]
	if !ok {
		return nil, fmt.Errorf("session: unknown provider %q", providerName)
	}

	return &Manager{p: p}, nil
}

func (this *Manager) Subscribe(topic []byte, qos byte, subscriber interface{}) (byte, error) {
	return this.p.Subscribe(topic, qos, subscriber)
}

func (this *Manager) Unsubscribe(topic []byte, subscriber interface{}) error {
	return this.p.Unsubscribe(topic, subscriber)
}

func (this *Manager) Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error {
	return this.p.Subscribers(topic, qos, subs, qoss)
}

func (this *Manager) Retain(msg *packets.PublishPacket) error {
	return this.p.Retain(msg)
}

func (this *Manager) Retained(topic []byte, msgs *[]*packets.PublishPacket) error {
	return this.p.Retained(topic, msgs)
}

func (this *Manager) Close() error {
	return this.p.Close()
}
