package broker

import (
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"time"

	mqtt5 "github.com/eclipse/paho.golang/packets"
	jsoniter "github.com/json-iterator/go"

	"github.com/fhmq/hmq/v4/broker/lib/sessions"
	"github.com/fhmq/hmq/v4/broker/packets"
	uuid "github.com/google/uuid"
)

const (
	// ACCEPT_MIN_SLEEP is the minimum acceptable sleep times on temporary errors.
	ACCEPT_MIN_SLEEP = 100 * time.Millisecond
	// ACCEPT_MAX_SLEEP is the maximum acceptable sleep times on temporary errors
	ACCEPT_MAX_SLEEP = 10 * time.Second
	// DEFAULT_TLS_TIMEOUT
	DEFAULT_TLS_TIMEOUT = 5 * time.Second
)

const (
	CONNECT = uint8(iota + 1)
	CONNACK
	PUBLISH
	PUBACK
	PUBREC
	PUBREL
	PUBCOMP
	SUBSCRIBE
	SUBACK
	UNSUBSCRIBE
	UNSUBACK
	PINGREQ
	PINGRESP
	DISCONNECT
)
const (
	QosAtMostOnce byte = iota
	QosAtLeastOnce
	QosExactlyOnce
	QosFailure = 0x80
)

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

func GenUniqueId() string {
	id, err := uuid.NewRandom()
	if err != nil {
		log.Error("uuid.NewRandom() returned an error: " + err.Error())
	}
	return id.String()
}

func wrapPublishPacket(packet *packets.PublishPacket) *packets.PublishPacket {
	p := packet.Copy()
	wrapPayload := map[string]interface{}{
		"message_id": GenUniqueId(),
		"payload":    string(p.Payload),
	}
	b, _ := json.Marshal(wrapPayload)
	p.Payload = b
	return p
}

func unWrapPublishPacket(packet *packets.PublishPacket) *packets.PublishPacket {
	p := packet.Copy()
	if payload := jsoniter.Get(p.Payload, "payload").ToString(); payload != "" {
		p.Payload = []byte(payload)
	}
	return p
}

var ErrPacketIDExhausted = errors.New("outbound packet IDs exhausted")
var ErrInvalidDeliveryQoS = errors.New("invalid outbound delivery QoS")
var errDurableAccepted = errors.New("delivery durably accepted")

func publish(sub *subscription, original *packets.PublishPacket, qos byte) error {
	return publishDelivery(sub, original, qos, false)
}

func publishDelivery(sub *subscription, original *packets.PublishPacket, qos byte, retained bool) error {
	if retained && sub.client.durable() {
		unlock := sub.client.broker.lockClientConnect(sub.client.info.clientID)
		defer unlock()
		current, ok := sub.client.broker.clients.Load(sub.client.info.clientID)
		if !ok || current != sub.client {
			return errors.Join(net.ErrClosed, errWriteNotStarted)
		}
	}
	packet := original.Copy()
	if packet.Metadata != nil && !packet.Metadata.ExpiresAt.IsZero() && !time.Now().Before(packet.Metadata.ExpiresAt) {
		return nil
	}
	if sub.noLocal && packet.Metadata != nil && packet.Metadata.Origin == sub.client.info.clientID {
		return nil
	}
	packet.Qos = qos
	packet.Dup = false
	packet.Retain = retained || sub.retainAsPublished && original.Retain
	packet.MessageID = 0
	if sub.subscriptionID != 0 {
		if packet.Metadata == nil {
			packet.Metadata = &packets.PublishMetadata{}
		}
		if packet.Metadata.Properties == nil {
			packet.Metadata.Properties = &mqtt5.Properties{}
		}
		id := sub.subscriptionID
		packet.Metadata.Properties.SubscriptionIdentifier = &id
	}
	if sub.client.durable() && (qos == 1 || qos == 2) {
		return sub.client.enqueueSessionDelivery(packet)
	}
	return sub.client.sendPublication(packet, sub.share, false)
}

func (c *client) sendPublication(packet *packets.PublishPacket, shared bool, fromQueue bool) error {
	if packet.Metadata != nil && !packet.Metadata.ExpiresAt.IsZero() && !time.Now().Before(packet.Metadata.ExpiresAt) {
		return nil
	}
	if c.mqtt5 != nil {
		if err := c.writeClientPacket(io.Discard, packet); err != nil {
			if errors.Is(err, errMQTT5MessageExpired) || mqtt5Reason(err) == 0x95 {
				return nil
			}
			return err
		}
	}
	switch packet.Qos {
	case QosAtMostOnce:
		err := c.WriterPacket(packet)
		if errors.Is(err, errMQTT5MessageExpired) {
			return nil
		}
		return err
	case QosAtLeastOnce, QosExactlyOnce:
		c.outboundSendMu.Lock()
		defer c.outboundSendMu.Unlock()
		c.stateMu.Lock()
		if c.status == Disconnected {
			c.stateMu.Unlock()
			return errors.Join(net.ErrClosed, errWriteNotStarted)
		}
		c.inflightMu.Lock()
		if c.mqtt5 != nil && (len(c.inflight) >= int(c.mqtt5.receiveMaximum) || !fromQueue && (len(c.mqtt5.queued) > 0 || c.mqtt5.draining)) {
			c.inflightMu.Unlock()
			c.stateMu.Unlock()
			if len(c.mqtt5.queued) >= 256 || c.mqtt5.queuedBytes+publicationSize(packet) > 4<<20 {
				return errors.New("MQTT 5 outbound queue full")
			}
			c.mqtt5.queued = append(c.mqtt5.queued, packet)
			c.mqtt5.queuedBytes += publicationSize(packet)
			return nil
		}
		for id := 1; id <= 65535; id++ {
			if _, used := c.inflight[uint16(id)]; !used {
				packet.MessageID = uint16(id)
				break
			}
		}
		if packet.MessageID == 0 {
			c.inflightMu.Unlock()
			c.stateMu.Unlock()
			return ErrPacketIDExhausted
		}
		c.inflight[packet.MessageID] = &inflightElem{status: Publish, packet: packet, timestamp: time.Now().Unix()}
		if c.session != nil && !c.session.UpdateOutbound(c.outboundOwner, packet.MessageID, &sessions.OutboundMessage{Packet: packet}) {
			delete(c.inflight, packet.MessageID)
			c.inflightMu.Unlock()
			c.stateMu.Unlock()
			return errors.Join(net.ErrClosed, errWriteNotStarted)
		}
		c.inflightMu.Unlock()
		c.stateMu.Unlock()
		if err := c.persistOutbound(packet.MessageID, &sessions.OutboundMessage{Packet: packet}); err != nil {
			c.inflightMu.Lock()
			delete(c.inflight, packet.MessageID)
			if c.session != nil {
				c.session.UpdateOutbound(c.outboundOwner, packet.MessageID, nil)
			}
			c.inflightMu.Unlock()
			return err
		}
		if c.durable() && c.broker.durableRouteHook != nil {
			c.broker.durableRouteHook(c.info.clientID, "persisted")
		}
		err := c.WriterPacket(packet)
		if errors.Is(err, errMQTT5MessageExpired) {
			c.inflightMu.Lock()
			delete(c.inflight, packet.MessageID)
			c.inflightMu.Unlock()
			if c.session != nil {
				c.session.UpdateOutbound(c.outboundOwner, packet.MessageID, nil)
			}
			return nil
		}
		if shared && (errors.Is(err, errWriteNotStarted) || errors.Is(err, errWriteZeroBytes)) {
			c.inflightMu.Lock()
			delete(c.inflight, packet.MessageID)
			if c.session != nil {
				c.session.UpdateOutbound(c.outboundOwner, packet.MessageID, nil)
			}
			c.inflightMu.Unlock()
			return err
		}
		c.ensureRetryTimer()
		if err != nil && c.durable() {
			return errors.Join(err, errDurableAccepted)
		}
		return err
	default:
		return fmt.Errorf("%w: %d", ErrInvalidDeliveryQoS, packet.Qos)
	}
}

// timer for retry delivery
func (c *client) ensureRetryTimer(interval ...int64) {
	if c.mqtt5 != nil {
		return
	}

	c.retryTimerLock.Lock()
	defer c.retryTimerLock.Unlock()

	if c.retryTimer != nil {
		return
	}
	if c.isDisconnected() {
		return
	}

	if len(interval) > 1 {
		return
	}
	timerInterval := retryInterval
	if len(interval) == 1 {
		timerInterval = interval[0]
	}

	c.retryTimer = time.AfterFunc(time.Duration(timerInterval)*time.Second, c.retryDelivery)

	return
}

func (c *client) resetRetryTimer() {
	// lock mutex before reading retryTimer
	c.retryTimerLock.Lock()
	defer c.retryTimerLock.Unlock()

	if c.retryTimer == nil {
		return
	}

	// reset timer
	c.retryTimer = nil
}

func (c *client) retryDelivery() {
	c.resetRetryTimer()

	c.mu.Lock()
	connLost := c.conn == nil
	c.mu.Unlock()
	if connLost {
		return
	}

	c.inflightMu.Lock()
	ilen := len(c.inflight)
	if ilen == 0 {
		c.inflightMu.Unlock()
		return
	}

	type retryElem struct {
		id        uint16
		elem      *inflightElem
		status    InflightStatus
		packet    *packets.PublishPacket
		timestamp int64
	}

	toRetryEle := make([]retryElem, 0, ilen)
	for id, infEle := range c.inflight {
		toRetryEle = append(toRetryEle, retryElem{
			id:        id,
			elem:      infEle,
			status:    infEle.status,
			packet:    infEle.packet,
			timestamp: infEle.timestamp,
		})
	}
	c.inflightMu.Unlock()
	now := time.Now().Unix()

	for _, infEle := range toRetryEle {
		age := now - infEle.timestamp
		if age >= retryInterval {
			c.retryInflight(infEle.id, infEle.elem, infEle.status, infEle.packet, now)
		} else {
			if age < 0 {
				age = 0
			}
			c.ensureRetryTimer(retryInterval - age)
		}
	}
	c.ensureRetryTimer()
}

func (c *client) retryInflight(id uint16, expected *inflightElem, status InflightStatus, original *packets.PublishPacket, now int64) {
	c.outboundSendMu.Lock()
	defer c.outboundSendMu.Unlock()
	c.inflightMu.RLock()
	current := c.inflight[id]
	valid := current == expected && current != nil && current.status == status
	c.inflightMu.RUnlock()
	if !valid {
		return
	}
	if status == Publish {
		packet := *original
		packet.Dup = true
		_ = c.WriterPacket(&packet)
	} else if status == Pubrel {
		pubrel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
		pubrel.MessageID = original.MessageID
		_ = c.WriterPacket(pubrel)
	} else {
		return
	}
	c.updateInflightTimestamp(id, expected, status, now)
}

func (c *client) updateInflightTimestamp(id uint16, expected *inflightElem, status InflightStatus, timestamp int64) {
	c.inflightMu.Lock()
	defer c.inflightMu.Unlock()

	if infEle, ok := c.inflight[id]; ok && infEle == expected && infEle.status == status {
		infEle.timestamp = timestamp
	}
}
