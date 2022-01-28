package topics

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

const (
	QosAtMostOnce byte = iota
	QosAtLeastOnce
	QosExactlyOnce
	QosFailure = 0x80
)

var _ TopicsProvider = (*memTopics)(nil)

type memTopics struct {
	// Sub/unsub mutex
	smu sync.RWMutex
	// Subscription tree
	sroot *snode

	// Retained message mutex
	rmu sync.RWMutex
	// Retained messages topic tree
	rroot *rnode
}

func init() {
	Register("mem", NewMemProvider())
}

// NewMemProvider returns an new instance of the memTopics, which is implements the
// TopicsProvider interface. memProvider is a hidden struct that stores the topic
// subscriptions and retained messages in memory. The content is not persistend so
// when the server goes, everything will be gone. Use with care.
func NewMemProvider() *memTopics {
	return &memTopics{
		sroot: newSNode(),
		rroot: newRNode(),
	}
}

func ValidQos(qos byte) bool {
	return qos == QosAtMostOnce || qos == QosAtLeastOnce || qos == QosExactlyOnce
}

func (this *memTopics) Subscribe(topic []byte, qos byte, sub interface{}) (byte, error) {
	if !ValidQos(qos) {
		return QosFailure, fmt.Errorf("Invalid QoS %d", qos)
	}

	if sub == nil {
		return QosFailure, fmt.Errorf("Subscriber cannot be nil")
	}

	this.smu.Lock()
	defer this.smu.Unlock()

	if qos > QosExactlyOnce {
		qos = QosExactlyOnce
	}

	if err := this.sroot.sinsert(topic, qos, sub); err != nil {
		return QosFailure, err
	}

	return qos, nil
}

func (this *memTopics) Unsubscribe(topic []byte, sub interface{}) error {
	this.smu.Lock()
	defer this.smu.Unlock()

	return this.sroot.sremove(topic, sub)
}

// Subscribers Returned values will be invalidated by the next Subscribers call
func (this *memTopics) Subscribers(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error {
	if !ValidQos(qos) {
		return fmt.Errorf("Invalid QoS %d", qos)
	}

	this.smu.RLock()
	defer this.smu.RUnlock()

	*subs = (*subs)[0:0]
	*qoss = (*qoss)[0:0]

	return this.sroot.smatch(topic, qos, subs, qoss)
}

func (this *memTopics) Retain(msg *packets.PublishPacket) error {
	this.rmu.Lock()
	defer this.rmu.Unlock()

	// So apparently, at least according to the MQTT Conformance/Interoperability
	// Testing, that a payload of 0 means delete the retain message.
	// https://eclipse.org/paho/clients/testing/
	if len(msg.Payload) == 0 {
		return this.rroot.rremove([]byte(msg.TopicName))
	}

	return this.rroot.rinsertOrUpdate([]byte(msg.TopicName), msg)
}

func (this *memTopics) Retained(topic []byte, msgs *[]*packets.PublishPacket) error {
	this.rmu.RLock()
	defer this.rmu.RUnlock()

	return this.rroot.rmatch(topic, msgs)
}

func (this *memTopics) Close() error {
	this.sroot = nil
	this.rroot = nil
	return nil
}

// subscrition nodes
type snode struct {
	// If this is the end of the topic string, then add subscribers here
	subs []interface{}
	qos  []byte

	// Otherwise add the next topic level here
	snodes map[string]*snode
}

func newSNode() *snode {
	return &snode{
		snodes: make(map[string]*snode),
	}
}

func (this *snode) sinsert(topic []byte, qos byte, sub interface{}) error {
	// If there's no more topic levels, that means we are at the matching snode
	// to insert the subscriber. So let's see if there's such subscriber,
	// if so, update it. Otherwise insert it.
	if len(topic) == 0 {
		// Let's see if the subscriber is already on the list. If yes, update
		// QoS and then return.
		for i := range this.subs {
			if equal(this.subs[i], sub) {
				this.qos[i] = qos
				return nil
			}
		}

		// Otherwise add.
		this.subs = append(this.subs, sub)
		this.qos = append(this.qos, qos)

		return nil
	}

	// Not the last level, so let's find or create the next level snode, and
	// recursively call it's insert().

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Add snode if it doesn't already exist
	n, ok := this.snodes[level]
	if !ok {
		n = newSNode()
		this.snodes[level] = n
	}

	return n.sinsert(rem, qos, sub)
}

// This remove implementation ignores the QoS, as long as the subscriber
// matches then it's removed
func (this *snode) sremove(topic []byte, sub interface{}) error {
	// If the topic is empty, it means we are at the final matching snode. If so,
	// let's find the matching subscribers and remove them.
	if len(topic) == 0 {
		// If subscriber == nil, then it's signal to remove ALL subscribers
		if sub == nil {
			this.subs = this.subs[0:0]
			this.qos = this.qos[0:0]
			return nil
		}

		// If we find the subscriber then remove it from the list. Technically
		// we just overwrite the slot by shifting all other items up by one.
		for i := range this.subs {
			if equal(this.subs[i], sub) {
				this.subs = append(this.subs[:i], this.subs[i+1:]...)
				this.qos = append(this.qos[:i], this.qos[i+1:]...)
				return nil
			}
		}

		return fmt.Errorf("No topic found for subscriber")
	}

	// Not the last level, so let's find the next level snode, and recursively
	// call it's remove().

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Find the snode that matches the topic level
	n, ok := this.snodes[level]
	if !ok {
		return fmt.Errorf("No topic found")
	}

	// Remove the subscriber from the next level snode
	if err := n.sremove(rem, sub); err != nil {
		return err
	}

	// If there are no more subscribers and snodes to the next level we just visited
	// let's remove it
	if len(n.subs) == 0 && len(n.snodes) == 0 {
		delete(this.snodes, level)
	}

	return nil
}

// smatch() returns all the subscribers that are subscribed to the topic. Given a topic
// with no wildcards (publish topic), it returns a list of subscribers that subscribes
// to the topic. For each of the level names, it's a match
// - if there are subscribers to '#', then all the subscribers are added to result set
func (this *snode) smatch(topic []byte, qos byte, subs *[]interface{}, qoss *[]byte) error {
	// If the topic is empty, it means we are at the final matching snode. If so,
	// let's find the subscribers that match the qos and append them to the list.
	if len(topic) == 0 {
		this.matchQos(qos, subs, qoss)
		if mwcn, _ := this.snodes[MWC]; mwcn != nil {
			mwcn.matchQos(qos, subs, qoss)
		}
		return nil
	}

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	for k, n := range this.snodes {
		// If the key is "#", then these subscribers are added to the result set
		if k == MWC {
			n.matchQos(qos, subs, qoss)
		} else if k == SWC || k == level {
			if err := n.smatch(rem, qos, subs, qoss); err != nil {
				return err
			}
		}
	}

	return nil
}

// retained message nodes
type rnode struct {
	// If this is the end of the topic string, then add retained messages here
	msg *packets.PublishPacket
	// Otherwise add the next topic level here
	rnodes map[string]*rnode
}

func newRNode() *rnode {
	return &rnode{
		rnodes: make(map[string]*rnode),
	}
}

func (this *rnode) rinsertOrUpdate(topic []byte, msg *packets.PublishPacket) error {
	// If there's no more topic levels, that means we are at the matching rnode.
	if len(topic) == 0 {
		// Reuse the message if possible
		this.msg = msg

		return nil
	}

	// Not the last level, so let's find or create the next level snode, and
	// recursively call it's insert().

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Add snode if it doesn't already exist
	n, ok := this.rnodes[level]
	if !ok {
		n = newRNode()
		this.rnodes[level] = n
	}

	return n.rinsertOrUpdate(rem, msg)
}

// Remove the retained message for the supplied topic
func (this *rnode) rremove(topic []byte) error {
	// If the topic is empty, it means we are at the final matching rnode. If so,
	// let's remove the buffer and message.
	if len(topic) == 0 {
		this.msg = nil
		return nil
	}

	// Not the last level, so let's find the next level rnode, and recursively
	// call it's remove().

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	// Find the rnode that matches the topic level
	n, ok := this.rnodes[level]
	if !ok {
		return fmt.Errorf("No topic found")
	}

	// Remove the subscriber from the next level rnode
	if err := n.rremove(rem); err != nil {
		return err
	}

	// If there are no more rnodes to the next level we just visited let's remove it
	if len(n.rnodes) == 0 {
		delete(this.rnodes, level)
	}

	return nil
}

// rmatch() finds the retained messages for the topic and qos provided. It's somewhat
// of a reverse match compare to match() since the supplied topic can contain
// wildcards, whereas the retained message topic is a full (no wildcard) topic.
func (this *rnode) rmatch(topic []byte, msgs *[]*packets.PublishPacket) error {
	// If the topic is empty, it means we are at the final matching rnode. If so,
	// add the retained msg to the list.
	if len(topic) == 0 {
		if this.msg != nil {
			*msgs = append(*msgs, this.msg)
		}
		return nil
	}

	// ntl = next topic level
	ntl, rem, err := nextTopicLevel(topic)
	if err != nil {
		return err
	}

	level := string(ntl)

	if level == MWC {
		// If '#', add all retained messages starting this node
		this.allRetained(msgs)
	} else if level == SWC {
		// If '+', check all nodes at this level. Next levels must be matched.
		for _, n := range this.rnodes {
			if err := n.rmatch(rem, msgs); err != nil {
				return err
			}
		}
	} else {
		// Otherwise, find the matching node, go to the next level
		if n, ok := this.rnodes[level]; ok {
			if err := n.rmatch(rem, msgs); err != nil {
				return err
			}
		}
	}

	return nil
}

func (this *rnode) allRetained(msgs *[]*packets.PublishPacket) {
	if this.msg != nil {
		*msgs = append(*msgs, this.msg)
	}

	for _, n := range this.rnodes {
		n.allRetained(msgs)
	}
}

const (
	stateCHR byte = iota // Regular character
	stateMWC             // Multi-level wildcard
	stateSWC             // Single-level wildcard
	stateSEP             // Topic level separator
	stateSYS             // System level topic ($)
)

// Returns topic level, remaining topic levels and any errors
func nextTopicLevel(topic []byte) ([]byte, []byte, error) {
	s := stateCHR

	for i, c := range topic {
		switch c {
		case '/':
			if s == stateMWC {
				return nil, nil, fmt.Errorf("Multi-level wildcard found in topic and it's not at the last level")
			}

			if i == 0 {
				return []byte(SWC), topic[i+1:], nil
			}

			return topic[:i], topic[i+1:], nil

		case '#':
			if i != 0 {
				return nil, nil, fmt.Errorf("Wildcard character '#' must occupy entire topic level")
			}

			s = stateMWC

		case '+':
			if i != 0 {
				return nil, nil, fmt.Errorf("Wildcard character '+' must occupy entire topic level")
			}

			s = stateSWC

		// case '$':
		// 	if i == 0 {
		// 		return nil, nil, fmt.Errorf("Cannot publish to $ topics")
		// 	}

		// 	s = stateSYS

		default:
			if s == stateMWC || s == stateSWC {
				return nil, nil, fmt.Errorf("Wildcard characters '#' and '+' must occupy entire topic level")
			}

			s = stateCHR
		}
	}

	// If we got here that means we didn't hit the separator along the way, so the
	// topic is either empty, or does not contain a separator. Either way, we return
	// the full topic
	return topic, nil, nil
}

// The QoS of the payload messages sent in response to a subscription must be the
// minimum of the QoS of the originally published message (in this case, it's the
// qos parameter) and the maximum QoS granted by the server (in this case, it's
// the QoS in the topic tree).
//
// It's also possible that even if the topic matches, the subscriber is not included
// due to the QoS granted is lower than the published message QoS. For example,
// if the client is granted only QoS 0, and the publish message is QoS 1, then this
// client is not to be send the published message.
func (this *snode) matchQos(qos byte, subs *[]interface{}, qoss *[]byte) {
	for _, sub := range this.subs {
		// If the published QoS is higher than the subscriber QoS, then we skip the
		// subscriber. Otherwise, add to the list.
		// if qos >= this.qos[i] {
		*subs = append(*subs, sub)
		*qoss = append(*qoss, qos)
		// }
	}
}

func equal(k1, k2 interface{}) bool {
	if reflect.TypeOf(k1) != reflect.TypeOf(k2) {
		return false
	}

	if reflect.ValueOf(k1).Kind() == reflect.Func {
		return &k1 == &k2
	}

	if k1 == k2 {
		return true
	}

	switch k1 := k1.(type) {
	case string:
		return k1 == k2.(string)

	case int64:
		return k1 == k2.(int64)

	case int32:
		return k1 == k2.(int32)

	case int16:
		return k1 == k2.(int16)

	case int8:
		return k1 == k2.(int8)

	case int:
		return k1 == k2.(int)

	case float32:
		return k1 == k2.(float32)

	case float64:
		return k1 == k2.(float64)

	case uint:
		return k1 == k2.(uint)

	case uint8:
		return k1 == k2.(uint8)

	case uint16:
		return k1 == k2.(uint16)

	case uint32:
		return k1 == k2.(uint32)

	case uint64:
		return k1 == k2.(uint64)

	case uintptr:
		return k1 == k2.(uintptr)
	}

	return false
}
