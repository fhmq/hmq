package kafka

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"regexp"
	"time"

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
	Addr             []string `json:"addr"`
	ConnectTopic     string   `json:"onConnect"`
	SubscribeTopic   string   `json:"onSubscribe"`
	PublishTopic     string   `json:"onPublish"`
	UnsubscribeTopic string   `json:"onUnsubscribe"`
	DisconnectTopic  string   `json:"onDisconnect"`
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

const (
	_ThingModelTopicRegexp = `^/\$system/(.+)/(.+)/tmodel/(.*)$`
)

//Publish publish to kafka
func Publish(e *plugins.Elements) {
	topic, key := "", e.ClientID
	switch e.Action {
	case plugins.Connect:
		topic = config.ConnectTopic
	case plugins.Publish:
		topic = config.PublishTopic
	case plugins.Subscribe:
		topic = config.SubscribeTopic
	case plugins.Unsubscribe:
		topic = config.UnsubscribeTopic
	case plugins.Disconnect:
		topic = config.DisconnectTopic
	default:
		log.Error("error action: ", zap.String("action", e.Action))
		return
	}
	// fmt.Println("publish kafka: ", topic, key)
	err := publish(topic, key, e)
	if err != nil {
		log.Error("publish kafka error: ", zap.Error(err))
	}

	match, _ := regexp.MatchString(_ThingModelTopicRegexp, e.Topic)
	if match && e.Action == plugins.Publish {
		topic := "tmodel.msg.upstream"
		err := publish(topic, key, e)
		if err != nil {
			log.Error("publish kafka error: ", zap.Error(err))
		}
	}
}

func publish(topic, key string, msg *plugins.Elements) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	select {
	case kafkaClient.Input() <- &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(payload)}:
		return nil
	case <-time.After(1 * time.Minute):
		return errors.New("send to kafka time out")
	}
}
