package kafka

import (
	"encoding/json"
	"io/ioutil"
	"regexp"

	"github.com/Shopify/sarama"
	"github.com/fhmq/hmq/logger"
	"github.com/fhmq/hmq/plugins"
	"go.uber.org/zap"
)

const (
	//Kafka plugin name
	Kafka = "kafka"
)

var (
	kafkaClient sarama.AsyncProducer
	config      Config
	log         = logger.Get().Named("kafka")
)

//Config device kafka config
type Config struct {
	Addr             []string          `json:"addr"`
	ConnectTopic     string            `json:"onConnect"`
	SubscribeTopic   string            `json:"onSubscribe"`
	PublishTopic     string            `json:"onPublish"`
	UnsubscribeTopic string            `json:"onUnsubscribe"`
	DisconnectTopic  string            `json:"onDisconnect"`
	RegexpMap        map[string]string `json:"regexpMap"`
}

//Init init kafak client
func Init() {
	log.Info("start connect kafka....")
	content, err := ioutil.ReadFile("./plugins/kafka/kafka.json")
	if err != nil {
		log.Fatal("Read config file error: ", zap.Error(err))
	}
	// log.Info(string(content))

	err = json.Unmarshal(content, &config)
	if err != nil {
		log.Fatal("Unmarshal config file error: ", zap.Error(err))
	}
	connect()
}

//connect
func connect() {
	var err error
	conf := sarama.NewConfig()
	conf.Version = sarama.V1_1_1_0
	kafkaClient, err = sarama.NewAsyncProducer(config.Addr, conf)
	if err != nil {
		log.Fatal("create kafka async producer failed: ", zap.Error(err))
	}

	go func() {
		for err := range kafkaClient.Errors() {
			log.Error("send msg to kafka failed: ", zap.Error(err))
		}
	}()
}

//Publish publish to kafka
func Publish(e *plugins.Elements) {
	key := e.ClientID
	var topics []string
	switch e.Action {
	case plugins.Connect:
		if config.ConnectTopic != "" {
			topics = append(topics, config.ConnectTopic)
		}
	case plugins.Publish:
		if config.PublishTopic != "" {
			topics = append(topics, config.PublishTopic)
		}
		// foreach regexp map config
		for reg, topic := range config.RegexpMap {
			match, _ := regexp.MatchString(reg, e.Topic)
			if match {
				topics = append(topics, topic)
			}
		}
	case plugins.Subscribe:
		if config.SubscribeTopic != "" {
			topics = append(topics, config.SubscribeTopic)
		}
	case plugins.Unsubscribe:
		if config.UnsubscribeTopic != "" {
			topics = append(topics, config.UnsubscribeTopic)
		}
	case plugins.Disconnect:
		if config.DisconnectTopic != "" {
			topics = append(topics, config.DisconnectTopic)
		}
	default:
		log.Error("error action: ", zap.String("action", e.Action))
		return
	}

	err := publish(topics, key, e)
	if err != nil {
		log.Error("publish kafka error: ", zap.Error(err))
	}

}

func publish(topics []string, key string, msg *plugins.Elements) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	for _, topic := range topics {
		kafkaClient.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.ByteEncoder(key),
			Value: sarama.ByteEncoder(payload),
		}
	}

	return nil
}
