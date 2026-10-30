package bridge

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type kafkaConfig struct {
	Addr             []string          `json:"addr"`
	ConnectTopic     string            `json:"onConnect"`
	SubscribeTopic   string            `json:"onSubscribe"`
	PublishTopic     string            `json:"onPublish"`
	UnsubscribeTopic string            `json:"onUnsubscribe"`
	DisconnectTopic  string            `json:"onDisconnect"`
	DeliverMap       map[string]string `json:"deliverMap"`
}

type kafka struct {
	kafkaConfig kafkaConfig
	kafkaClient sarama.AsyncProducer
	mu          sync.RWMutex
	closeOnce   sync.Once
	done        chan struct{}
	errorsDone  chan struct{}
	closeErr    error
}

// InitKafka Init kafka client
func InitKafka() (*kafka, error) {
	log.Info("start connect kafka....")
	content, err := ioutil.ReadFile("./plugins/kafka/kafka.json")
	if err != nil {
		return nil, fmt.Errorf("read kafka config: %w", err)
	}
	// log.Info(string(content))
	var config kafkaConfig
	err = json.Unmarshal(content, &config)
	if err != nil {
		return nil, fmt.Errorf("parse kafka config: %w", err)
	}
	c := &kafka{kafkaConfig: config}
	if err := c.connect(); err != nil {
		return nil, err
	}
	return c, nil
}

// connect
func (k *kafka) connect() error {
	conf := sarama.NewConfig()
	conf.Version = sarama.V1_1_1_0
	kafkaClient, err := sarama.NewAsyncProducer(k.kafkaConfig.Addr, conf)
	if err != nil {
		return fmt.Errorf("create kafka async producer: %w", err)
	}
	k.kafkaClient = kafkaClient
	k.done = make(chan struct{})
	k.errorsDone = make(chan struct{})
	go func() {
		defer close(k.errorsDone)
		for err := range kafkaClient.Errors() {
			log.Error("send msg to kafka failed: ", zap.Error(err))
		}
	}()

	return nil
}

func (k *kafka) Close() error {
	k.closeOnce.Do(func() {
		close(k.done)
		k.mu.Lock()
		defer k.mu.Unlock()
		k.kafkaClient.AsyncClose()
		<-k.errorsDone
	})
	return k.closeErr
}

// Publish publish to kafka
func (k *kafka) Publish(e *Elements) (bool, error) {
	config := k.kafkaConfig
	key := e.ClientID
	topics := make(map[string]bool)
	switch e.Action {
	case Connect:
		if config.ConnectTopic != "" {
			topics[config.ConnectTopic] = true
		}
	case Publish:
		if config.PublishTopic != "" {
			topics[config.PublishTopic] = true
		}
		// foreach regexp map config
		for reg, topic := range config.DeliverMap {
			match := matchTopic(reg, e.Topic)
			if match {
				topics[topic] = true
			}
		}
	case Subscribe:
		if config.SubscribeTopic != "" {
			topics[config.SubscribeTopic] = true
		}
	case Unsubscribe:
		if config.UnsubscribeTopic != "" {
			topics[config.UnsubscribeTopic] = true
		}
	case Disconnect:
		if config.DisconnectTopic != "" {
			topics[config.DisconnectTopic] = true
		}
	default:
		return false, errors.New("error action: " + e.Action)
	}

	return false, k.publish(topics, key, e)

}

func (k *kafka) publish(topics map[string]bool, key string, msg *Elements) error {
	k.mu.RLock()
	defer k.mu.RUnlock()
	select {
	case <-k.done:
		return errors.New("kafka producer closed")
	default:
	}
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	for topic, _ := range topics {
		select {
		case <-k.done:
			return errors.New("kafka producer closed")
		case k.kafkaClient.Input() <- &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.ByteEncoder(key),
			Value: sarama.ByteEncoder(payload),
		}:
			continue
		case <-time.After(5 * time.Second):
			return errors.New("write kafka timeout")
		}

	}

	return nil
}

func match(subTopic []string, topic []string) bool {
	if len(subTopic) == 0 {
		if len(topic) == 0 {
			return true
		}
		return false
	}

	if len(topic) == 0 {
		if subTopic[0] == "#" {
			return true
		}
		return false
	}

	if subTopic[0] == "#" {
		return true
	}

	if (subTopic[0] == "+") || (subTopic[0] == topic[0]) {
		return match(subTopic[1:], topic[1:])
	}
	return false
}

func matchTopic(subTopic string, topic string) bool {
	if subTopic == "" || topic == "" || len(subTopic) > 65535 || len(topic) > 65535 ||
		!utf8.ValidString(subTopic) || !utf8.ValidString(topic) ||
		strings.ContainsAny(topic, "#+\x00") || strings.ContainsRune(subTopic, '\x00') {
		return false
	}
	levels := strings.Split(subTopic, "/")
	for i, level := range levels {
		if strings.Contains(level, "#") && (level != "#" || i != len(levels)-1) {
			return false
		}
		if strings.Contains(level, "+") && level != "+" {
			return false
		}
	}
	if strings.HasPrefix(topic, "$") && (levels[0] == "+" || levels[0] == "#") {
		return false
	}
	return match(levels, strings.Split(topic, "/"))
}
