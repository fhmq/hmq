package bridge

import (
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

type lifecycleProducer struct {
	sarama.AsyncProducer
	input  chan *sarama.ProducerMessage
	errors chan *sarama.ProducerError
	closes int
}

func (p *lifecycleProducer) Input() chan<- *sarama.ProducerMessage { return p.input }
func (p *lifecycleProducer) AsyncClose()                           { p.closes++; close(p.errors) }

func TestKafkaCloseInterruptsPublish(t *testing.T) {
	p := &lifecycleProducer{input: make(chan *sarama.ProducerMessage), errors: make(chan *sarama.ProducerError)}
	k := &kafka{kafkaClient: p, done: make(chan struct{}), errorsDone: make(chan struct{})}
	go func() {
		for range p.errors {
		}
		close(k.errorsDone)
	}()
	result := make(chan error, 1)
	go func() { result <- k.publish(map[string]bool{"topic": true}, "key", &Elements{}) }()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := k.Close(); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("publish succeeded after close")
		}
	case <-time.After(time.Second):
		t.Fatal("publish blocked")
	}
	if p.closes != 1 {
		t.Fatalf("close called %d times", p.closes)
	}
	if err := k.publish(nil, "", &Elements{}); err == nil {
		t.Fatal("publish after close succeeded")
	}
}

func TestInitKafkaMissingConfig(t *testing.T) {
	if _, err := InitKafka(); err == nil {
		t.Fatal("expected missing config error")
	}
}
