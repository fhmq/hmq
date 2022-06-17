package broker

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"github.com/tidwall/gjson"
	"go.uber.org/zap"

	"hmq/broker/packets"

	uuid "github.com/google/uuid"
)

const (
	// ACCEPT_MIN_SLEEP is the minimum acceptable sleep times on temporary errors.
	ACCEPT_MIN_SLEEP = 100 * time.Millisecond
	// ACCEPT_MAX_SLEEP is the maximum acceptable sleep times on temporary errors
	ACCEPT_MAX_SLEEP = 10 * time.Second
	// DEFAULT_ROUTE_CONNECT Route solicitation intervals.
	DEFAULT_ROUTE_CONNECT = 5 * time.Second
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

func addSubMap(m map[string]uint64, topic string) {
	subNum, exist := m[topic]
	if exist {
		m[topic] = subNum + 1
	} else {
		m[topic] = 1
	}
}

func delSubMap(m map[string]uint64, topic string) uint64 {
	subNum, exist := m[topic]
	if exist {
		if subNum > 1 {
			m[topic] = subNum - 1
			return subNum - 1
		}
	} else {
		m[topic] = 0
	}

	return 0
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
	if gjson.GetBytes(p.Payload, "payload").Exists() {
		p.Payload = []byte(gjson.GetBytes(p.Payload, "payload").String())
	}
	return p
}

// publishToSubscribers function publishes the payload to all matching subscribers
func publishToSubscribers(sub *subscription, packet *packets.PublishPacket, ctx context.Context, cancel context.CancelFunc) {
	switch packet.Qos {
	case QosAtMostOnce:
		err := sub.client.WriterPacket(packet, ctx, cancel)
		if err != nil {
			log.Error("process message for psub error,  ", zap.Error(err))
		}
	case QosAtLeastOnce, QosExactlyOnce:
		sub.client.inflightMu.Lock()
		sub.client.inflight[packet.MessageID] = &inflightElem{status: Publish, packet: packet, timestamp: time.Now().Unix()}
		sub.client.inflightMu.Unlock()
		err := sub.client.WriterPacket(packet, ctx, cancel)
		if err != nil {
			log.Error("process message for psub error,  ", zap.Error(err))
		}
		sub.client.ensureRetryTimer(ctx, cancel)
	default:
		log.Error("publish with unknown qos", zap.String("ClientID", sub.client.info.clientID))
		return
	}
}

// timer for retry delivery
func (c *client) ensureRetryTimer(ctx context.Context, cancel context.CancelFunc, interval ...int64) {

	c.retryTimerLock.Lock()
	defer c.retryTimerLock.Unlock()

	if c.retryTimer != nil {
		return
	}

	if len(interval) > 1 {
		return
	}
	timerInterval := retryInterval
	if len(interval) == 1 {
		timerInterval = interval[0]
	}

	var retryFunc = func() {
		c.retryDelivery(ctx, cancel)
	}

	c.retryTimer = time.AfterFunc(time.Duration(timerInterval)*time.Second, retryFunc)

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

func (c *client) retryDelivery(ctx context.Context, cancel context.CancelFunc) {
	c.resetRetryTimer()
	c.inflightMu.RLock()
	ilen := len(c.inflight)

	if c.mu.Lock(); c.conn == nil || ilen == 0 { //Reset timer when client offline OR inflight is empty
		c.inflightMu.RUnlock()
		c.mu.Unlock()
		return
	}

	// copy the to be retried elements out of the map to only hold the lock for a short time and use the new slice later to iterate
	// through them
	toRetryEle := make([]*inflightElem, 0, ilen)
	for _, infEle := range c.inflight {
		toRetryEle = append(toRetryEle, infEle)
	}
	c.inflightMu.RUnlock()
	now := time.Now().Unix()

	for _, infEle := range toRetryEle {
		age := now - infEle.timestamp
		if age >= retryInterval {
			if infEle.status == Publish {
				fail := c.WriterPacket(infEle.packet, ctx, cancel)
				if fail != nil {
					log.Error("failed to deliver PUBLISH (qos 2)", zap.String("ClientID", c.info.clientID))
					continue
				}
				infEle.timestamp = now
			} else if infEle.status == PubRel {
				pubrel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
				pubrel.MessageID = infEle.packet.MessageID
				fail := c.WriterPacket(pubrel, ctx, cancel)
				if fail != nil {
					log.Error("failed to deliver PUBREL packet", zap.String("ClientID", c.info.clientID))
					continue
				}
				infEle.timestamp = now
			}
		} else {
			if age < 0 {
				age = 0
			}
			c.ensureRetryTimer(ctx, cancel, retryInterval-age)
		}
	}
	c.ensureRetryTimer(ctx, cancel)
}
