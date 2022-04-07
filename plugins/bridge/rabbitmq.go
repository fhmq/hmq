package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"sync"
	"sync/atomic"
	"time"

	"github.com/streadway/amqp"
	"go.uber.org/zap"
)

type RabbitmqBridgeConfig struct {
	Addr             string            `json:"addr"`
	Exchange         string            `json:"exchange"`
	RoutingKey       string            `json:"routingKey"`
	Username         string            `json:"username"`
	Password         string            `json:"password"`
	ConnectTopic     string            `json:"onConnect"`
	SubscribeTopic   string            `json:"onSubscribe"`
	PublishTopic     string            `json:"onPublish"`
	UnsubscribeTopic string            `json:"onUnsubscribe"`
	DisconnectTopic  string            `json:"onDisconnect"`
	DeliverMap       map[string]string `json:"deliverMap"`
}

func InitRabbitmq(confFilepath string) *RabbitmqBridge {
	log.Info("start connect rabbitmq....")
	content, err := ioutil.ReadFile(confFilepath)
	if err != nil {
		log.Fatal("Read config file error: ", zap.Error(err))
	}
	conf := new(RabbitmqBridgeConfig)
	err = json.Unmarshal(content, conf)
	if err != nil {
		log.Fatal("Unmarshal config file error: ", zap.Error(err))
	}

	client := NewRabbitmqClient(conf.Addr, conf.Username, conf.Password)
	err = client.InitConn()
	if err != nil {
		log.Sugar().Fatalf("rabbitmq connected error: %s", err)
	}

	err = client.DeclareStandardDirectExchange(conf.Exchange)
	if err != nil {
		log.Sugar().Fatalf("rabbitmq declare exchange error: %s", err)
	}

	return &RabbitmqBridge{
		client: client,
		config: conf,
	}
}

type RabbitmqBridge struct {
	config *RabbitmqBridgeConfig
	client *RabbitmqClient
}

func (a *RabbitmqBridge) Publish(e *Elements) error {
	return a.publish(a.config.Exchange, a.config.RoutingKey, e)
}

func (a *RabbitmqBridge) publish(exchange, key string, msg *Elements) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return a.client.Publish(exchange, key, payload)
}

func NewRabbitmqClient(addr, username, password string) *RabbitmqClient {
	reconnCtx, cancel := context.WithCancel(context.Background())
	client := &RabbitmqClient{
		url:          fmt.Sprintf("amqp://%s:%s@%s", username, password, addr),
		cancelReconn: cancel,
		reconnCtx:    reconnCtx,
		closeCh:      make(chan struct{}),
	}
	return client
}

type RabbitmqClient struct {
	url           string
	conn          *amqp.Connection
	mqCh          *amqp.Channel
	reConnFlag    uint32
	currReConnNum int

	mu           sync.Mutex
	closeCh      chan struct{}
	cancelReconn context.CancelFunc
	reconnCtx    context.Context
}

func (rc *RabbitmqClient) InitConn() (err error) {
	rc.conn, err = amqp.Dial(rc.url)
	if err != nil {
		return
	}
	rc.mqCh, err = rc.conn.Channel()
	return
}

func (rc *RabbitmqClient) reConn() error {
	log.Info("rabbitmq start reconnecting...")
	var err error
	rc.mu.Lock()
	if !atomic.CompareAndSwapUint32(&rc.reConnFlag, 0, 1) {
		log.Debug("waiting for reconnecting...")
		rc.mu.Unlock()
		<-rc.reconnCtx.Done()
		return nil
	}
	rc.reconnCtx, rc.cancelReconn = context.WithCancel(context.Background())
	rc.mu.Unlock()
	for {
		log.Sugar().Infof("MQ %dth Reconnecting...", rc.currReConnNum+1)
		if rc.conn == nil || rc.conn.IsClosed() {
			err = rc.InitConn()
			if err != nil {
				log.Sugar().Warnf("rabbitmq reConn > InitConn > %s", err)
				rc.currReConnNum++
				time.Sleep(time.Second * 10)
				continue
			}
		}
		rc.currReConnNum = 0
		atomic.StoreUint32(&rc.reConnFlag, 0)
		log.Info("MQ Reconnected Successfully")
		rc.cancelReconn()
		return nil
	}
}

func (rc *RabbitmqClient) DeclareStandardDirectExchange(name string) error {
	return rc.mqCh.ExchangeDeclare(name, "direct", true, false, false, true, nil)
}

func (rc *RabbitmqClient) Publish(ex, key string, body []byte) error {
	if atomic.LoadUint32(&rc.reConnFlag) == 1 {
		return nil
	}

	err := rc.mqCh.Publish(ex, key, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Priority:     0,
		Expiration:   "",
		Body:         body,
	})

	if err != nil {
		go rc.reConn()
	}
	return err
}

func (rc *RabbitmqClient) Close() error {
	close(rc.closeCh)
	if rc.conn != nil {
		return rc.conn.Close()
	}
	return nil
}
