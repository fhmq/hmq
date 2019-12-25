package broker

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/tidwall/gjson"
	"go.uber.org/zap"

	"github.com/eclipse/paho.mqtt.golang/packets"
	uuid "github.com/satori/go.uuid"
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
	return uuid.NewV4().String()
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

func publish(sub *subscription, packet *packets.PublishPacket) {
	// var p *packets.PublishPacket
	// if sub.client.info.username != "root" {
	// 	p = unWrapPublishPacket(packet)
	// } else {
	// 	p = wrapPublishPacket(packet)
	// }
	// err := sub.client.WriterPacket(p)
	// if err != nil {
	// 	log.Error("process message for psub error,  ", zap.Error(err))
	// }

	err := sub.client.WriterPacket(packet)
	if err != nil {
		log.Error("process message for psub error,  ", zap.Error(err))
	}
}
