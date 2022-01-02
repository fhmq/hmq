package sessions

import (
	"fmt"
	"sync"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	// Queue size for the ack queue
	defaultQueueSize = 16
)

type Session struct {

	// cmsg is the CONNECT message
	cmsg *packets.ConnectPacket

	// Will message to publish if connect is closed unexpectedly
	Will *packets.PublishPacket

	// Retained publish message
	Retained *packets.PublishPacket

	// topics stores all the topis for this session/client
	topics map[string]byte

	// Initialized?
	initted bool

	// Serialize access to this session
	mu sync.Mutex

	id string
}

func (this *Session) Init(msg *packets.ConnectPacket) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if this.initted {
		return fmt.Errorf("Session already initialized")
	}

	this.cmsg = msg

	if this.cmsg.WillFlag {
		this.Will = packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		this.Will.Qos = this.cmsg.Qos
		this.Will.TopicName = this.cmsg.WillTopic
		this.Will.Payload = this.cmsg.WillMessage
		this.Will.Retain = this.cmsg.WillRetain
	}

	this.topics = make(map[string]byte, 1)

	this.id = msg.ClientIdentifier

	this.initted = true

	return nil
}

func (this *Session) Update(msg *packets.ConnectPacket) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.cmsg = msg
	return nil
}

func (this *Session) RetainMessage(msg *packets.PublishPacket) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.Retained = msg

	return nil
}

func (this *Session) AddTopic(topic string, qos byte) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if !this.initted {
		return fmt.Errorf("Session not yet initialized")
	}

	this.topics[topic] = qos

	return nil
}

func (this *Session) RemoveTopic(topic string) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if !this.initted {
		return fmt.Errorf("Session not yet initialized")
	}

	delete(this.topics, topic)

	return nil
}

func (this *Session) Topics() ([]string, []byte, error) {
	this.mu.Lock()
	defer this.mu.Unlock()

	if !this.initted {
		return nil, nil, fmt.Errorf("Session not yet initialized")
	}

	var (
		topics []string
		qoss   []byte
	)

	for k, v := range this.topics {
		topics = append(topics, k)
		qoss = append(qoss, v)
	}

	return topics, qoss, nil
}

func (this *Session) ID() string {
	return this.cmsg.ClientIdentifier
}

func (this *Session) WillFlag() bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.cmsg.WillFlag
}

func (this *Session) SetWillFlag(v bool) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.cmsg.WillFlag = v
}

func (this *Session) CleanSession() bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.cmsg.CleanSession
}
