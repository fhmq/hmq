package sessions

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/fhmq/hmq/v4/broker/packets"
)

var subscriptionGeneration atomic.Uint64

// NewSubscriptionGeneration identifies one uninterrupted subscription lifetime.
func NewSubscriptionGeneration() uint64 { return subscriptionGeneration.Add(1) }

const (
	// Queue size for the ack queue
	defaultQueueSize = 16
)

type Session struct {

	// cmsg is the CONNECT message
	cmsg *packets.ConnectPacket

	// Will message to publish if connect is closed unexpectedly
	will *packets.PublishPacket

	// Retained publish message
	retained *packets.PublishPacket

	// topics stores all the topis for this session/client
	topics           map[string]byte
	topicGenerations map[string]uint64

	// Initialized?
	initted bool

	// Serialize access to this session
	mu sync.Mutex

	id string

	// inbound QoS2 state is retained for the lifetime of this in-memory session.
	awaitingRel map[uint16]int64

	// outbound exists only for the lifetime of this in-memory session.
	outbound map[uint16]OutboundMessage
	owner    uint64
}

// ClaimOutbound invalidates updates from all previous connections.
func (this *Session) ClaimOutbound() (uint64, map[uint16]OutboundMessage) {
	this.mu.Lock()
	defer this.mu.Unlock()
	this.owner++
	result := make(map[uint16]OutboundMessage, len(this.outbound))
	for id, msg := range this.outbound {
		msg.Packet = clonePublish(msg.Packet)
		result[id] = msg
	}
	return this.owner, result
}

func (this *Session) UpdateOutbound(owner uint64, id uint16, msg *OutboundMessage) bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	if owner != this.owner {
		return false
	}
	if msg == nil {
		delete(this.outbound, id)
		return true
	}
	if this.outbound == nil {
		this.outbound = make(map[uint16]OutboundMessage)
	}
	copy := *msg
	copy.Packet = clonePublish(msg.Packet)
	this.outbound[id] = copy
	return true
}

// OutboundMessage describes a delivery awaiting PUBACK, PUBREC or PUBCOMP.
type OutboundMessage struct {
	Packet          *packets.PublishPacket
	AwaitingPubcomp bool
}

// SetOutbound stores an independent copy of an unacknowledged delivery.
func (this *Session) SetOutbound(id uint16, msg OutboundMessage) {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.outbound == nil {
		this.outbound = make(map[uint16]OutboundMessage)
	}
	msg.Packet = clonePublish(msg.Packet)
	this.outbound[id] = msg
}

// RemoveOutbound removes an acknowledged delivery.
func (this *Session) RemoveOutbound(id uint16) {
	this.mu.Lock()
	defer this.mu.Unlock()
	delete(this.outbound, id)
}

// Outbound returns a snapshot which can be modified without changing the session.
func (this *Session) Outbound() map[uint16]OutboundMessage {
	this.mu.Lock()
	defer this.mu.Unlock()
	result := make(map[uint16]OutboundMessage, len(this.outbound))
	for id, msg := range this.outbound {
		msg.Packet = clonePublish(msg.Packet)
		result[id] = msg
	}
	return result
}

// RegisterAwaitingRel atomically reserves an inbound QoS2 packet identifier.
func (this *Session) RegisterAwaitingRel(id uint16, timestamp int64, limit int) int {
	this.mu.Lock()
	defer this.mu.Unlock()
	if _, ok := this.awaitingRel[id]; ok {
		return 1
	}
	if limit > 0 && len(this.awaitingRel) >= limit {
		return 2
	}
	if this.awaitingRel == nil {
		this.awaitingRel = make(map[uint16]int64)
	}
	this.awaitingRel[id] = timestamp
	return 0
}

func (this *Session) RemoveAwaitingRel(id uint16) bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	_, ok := this.awaitingRel[id]
	delete(this.awaitingRel, id)
	return ok
}

func (this *Session) AwaitingRelCount() int {
	this.mu.Lock()
	defer this.mu.Unlock()
	return len(this.awaitingRel)
}

func (this *Session) Init(msg *packets.ConnectPacket) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if msg == nil {
		return fmt.Errorf("Session connect message is nil")
	}
	if this.initted {
		return fmt.Errorf("Session already initialized")
	}

	this.cmsg = cloneConnect(msg)

	if this.cmsg.WillFlag {
		this.will = packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		this.will.Qos = this.cmsg.Qos
		this.will.TopicName = this.cmsg.WillTopic
		this.will.Payload = append([]byte(nil), this.cmsg.WillMessage...)
		this.will.Retain = this.cmsg.WillRetain
	}

	this.topics = make(map[string]byte, 1)

	this.id = msg.ClientIdentifier

	this.initted = true

	return nil
}

func (this *Session) Update(msg *packets.ConnectPacket) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if !this.initted || msg == nil {
		return fmt.Errorf("Session not initialized or connect message is nil")
	}
	this.cmsg = cloneConnect(msg)
	return nil
}

func (this *Session) RetainMessage(msg *packets.PublishPacket) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	this.retained = clonePublish(msg)

	return nil
}

// Will returns an independent snapshot of the will message, or nil.
func (this *Session) Will() *packets.PublishPacket {
	this.mu.Lock()
	defer this.mu.Unlock()
	return clonePublish(this.will)
}

// Retained returns an independent snapshot of the retained message, or nil.
func (this *Session) Retained() *packets.PublishPacket {
	this.mu.Lock()
	defer this.mu.Unlock()
	return clonePublish(this.retained)
}

func clonePublish(msg *packets.PublishPacket) *packets.PublishPacket {
	if msg == nil {
		return nil
	}
	copy := *msg
	copy.Payload = append([]byte(nil), msg.Payload...)
	copy.Metadata = packets.CloneMetadata(msg.Metadata)
	return &copy
}

func cloneConnect(msg *packets.ConnectPacket) *packets.ConnectPacket {
	copy := *msg
	copy.WillMessage = append([]byte(nil), msg.WillMessage...)
	copy.Password = append([]byte(nil), msg.Password...)
	return &copy
}

func (this *Session) AddTopic(topic string, qos byte) error {
	return this.AddTopicGeneration(topic, qos, NewSubscriptionGeneration())
}

func (this *Session) AddTopicGeneration(topic string, qos byte, generation uint64) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if !this.initted {
		return fmt.Errorf("Session not yet initialized")
	}

	this.topics[topic] = qos
	if this.topicGenerations == nil {
		this.topicGenerations = make(map[string]uint64)
	}
	this.topicGenerations[topic] = generation

	return nil
}

func (this *Session) RemoveTopic(topic string) error {
	this.mu.Lock()
	defer this.mu.Unlock()

	if !this.initted {
		return fmt.Errorf("Session not yet initialized")
	}

	delete(this.topics, topic)
	delete(this.topicGenerations, topic)

	return nil
}

func (this *Session) TopicGeneration(topic string) uint64 {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.topicGenerations[topic]
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
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.cmsg != nil {
		return this.cmsg.ClientIdentifier
	}
	return this.id
}

func (this *Session) WillFlag() bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.cmsg != nil && this.cmsg.WillFlag
}

func (this *Session) SetWillFlag(v bool) {
	this.mu.Lock()
	defer this.mu.Unlock()
	if this.cmsg != nil {
		this.cmsg.WillFlag = v
	}
}

func (this *Session) CleanSession() bool {
	this.mu.Lock()
	defer this.mu.Unlock()
	return this.cmsg != nil && this.cmsg.CleanSession
}
