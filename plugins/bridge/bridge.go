package bridge

import "github.com/fhmq/rhmq/logger"

const (
	//Connect mqtt connect
	Connect = "connect"
	//Publish mqtt publish
	Publish = "publish"
	//Subscribe mqtt sub
	Subscribe = "subscribe"
	//Unsubscribe mqtt sub
	Unsubscribe = "unsubscribe"
	//Disconnect mqtt disconenct
	Disconnect = "disconnect"
)

var (
	log = logger.Get().Named("bridge")
)

//Elements kafka publish elements
type Elements struct {
	ClientID  string `json:"clientid"`
	Username  string `json:"username"`
	Topic     string `json:"topic"`
	Payload   string `json:"payload"`
	Timestamp int64  `json:"ts"`
	Size      int32  `json:"size"`
	Action    string `json:"action"`
}

const (
	//Kafka plugin name
	Kafka = "kafka"
)

type BridgeMQ interface {
	Publish(e *Elements) error
}

func NewBridgeMQ(name string) BridgeMQ {
	switch name {
	case Kafka:
		return InitKafka()
	default:
		return &mockMQ{}
	}
}
