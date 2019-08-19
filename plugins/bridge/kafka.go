package bridge

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"regexp"

	"github.com/Shopify/sarama"
	"go.uber.org/zap"
)

type kafakConfig struct {
	Addr             []string          `json:"addr"`
	ConnectTopic     string            `json:"onConnect"`
	SubscribeTopic   string            `json:"onSubscribe"`
	PublishTopic     string            `json:"onPublish"`
	UnsubscribeTopic string            `json:"onUnsubscribe"`
	DisconnectTopic  string            `json:"onDisconnect"`
	RegexpMap        map[string]string `json:"regexpMap"`
}

type kafka struct {
	kafakConfig kafakConfig
	kafkaClient sarama.AsyncProducer
}

//Init init kafak client
func InitKafka() *kafka {
	log.Info("start connect kafka....")
	content, err := ioutil.ReadFile("./plugins/mq/kafka/kafka.json")
	if err != nil {
		log.Fatal("Read config file error: ", zap.Error(err))
	}
	// log.Info(string(content))
	var config kafakConfig
	err = json.Unmarshal(content, &config)
	if err != nil {
		log.Fatal("Unmarshal config file error: ", zap.Error(err))
	}
	c := &kafka{kafakConfig: config}
	c.connect()
	return c
}

//connect
func (k *kafka) connect() {
	conf := sarama.NewConfig()
	conf.Version = sarama.V1_1_1_0
	kafkaClient, err := sarama.NewAsyncProducer(k.kafakConfig.Addr, conf)
	if err != nil {
		log.Fatal("create kafka async producer failed: ", zap.Error(err))
	}

	go func() {
		for err := range kafkaClient.Errors() {
			log.Error("send msg to kafka failed: ", zap.Error(err))
		}
	}()

	k.kafkaClient = kafkaClient
}

//Publish publish to kafka
func (k *kafka) Publish(e *Elements) error {
	config := k.kafakConfig
	key := e.ClientID
	var topics []string
	switch e.Action {
	case Connect:
		if config.ConnectTopic != "" {
			topics = append(topics, config.ConnectTopic)
		}
	case Publish:
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
	case Subscribe:
		if config.SubscribeTopic != "" {
			topics = append(topics, config.SubscribeTopic)
		}
	case Unsubscribe:
		if config.UnsubscribeTopic != "" {
			topics = append(topics, config.UnsubscribeTopic)
		}
	case Disconnect:
		if config.DisconnectTopic != "" {
			topics = append(topics, config.DisconnectTopic)
		}
	default:
		return errors.New("error action: " + e.Action)
	}

	return k.publish(topics, key, e)

}

func (k *kafka) publish(topics []string, key string, msg *Elements) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	for _, topic := range topics {
		k.kafkaClient.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.ByteEncoder(key),
			Value: sarama.ByteEncoder(payload),
		}
	}

	return nil
}
