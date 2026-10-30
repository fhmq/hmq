package broker

import (
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/v3/broker/reliable/inbox"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type failingInboxQueue struct{}

func (failingInboxQueue) Pending(string, int) ([]inbox.Message, error) {
	return nil, errors.New("sensitive path /secret")
}

func (failingInboxQueue) Complete(string, string) error { return nil }

func TestHTTPReliableClusterHealth(t *testing.T) {
	consumer, err := inbox.NewConsumer(failingInboxQueue{}, "peer", time.Millisecond, func(inbox.Message) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	deadline := time.Now().Add(time.Second)
	for consumer.Err() == nil && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if consumer.Err() == nil {
		t.Fatal("consumer did not report failure")
	}
	runtime := &reliableRuntime{consumers: []*inbox.Consumer{consumer}}
	for _, tc := range []struct {
		name string
		b    *Broker
		want string
	}{
		{"disabled", &Broker{}, `"state":"disabled"`},
		{"running", &Broker{reliableRuntime: &reliableRuntime{}}, `"state":"running","failedLinks":0,"failedInboxConsumers":0`},
		{"degraded", &Broker{reliableRuntime: runtime}, `"state":"degraded","failedLinks":0,"failedInboxConsumers":1`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			monitorRouter(tc.b).ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/cluster/reliable", nil))
			if w.Code != 200 || !strings.Contains(w.Body.String(), tc.want) || strings.Contains(w.Body.String(), "secret") {
				t.Fatalf("unexpected response: %d %s", w.Code, w.Body.String())
			}
		})
	}
	runtime.mu.Lock()
	runtime.closed = true
	runtime.mu.Unlock()
	if got := runtime.health(); got.State != "closed" {
		t.Fatalf("closed health: %+v", got)
	}
}

func TestReliableHealthConcurrentClose(t *testing.T) {
	runtime := &reliableRuntime{}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			runtime.health()
		}
	}()
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	if got := runtime.health(); got.State != "closed" {
		t.Fatalf("closed health: %+v", got)
	}
}

func TestHTTPClusterDisabled(t *testing.T) {
	r := httptest.NewRecorder()
	monitorRouter(&Broker{}).ServeHTTP(r, httptest.NewRequest("GET", "/api/v1/cluster", nil))
	if r.Code != 200 || !strings.Contains(r.Body.String(), `"state":"disabled"`) || !strings.Contains(r.Body.String(), `"peers":[]`) || !strings.Contains(r.Body.String(), `"errorsAvailable":false`) {
		t.Fatalf("unexpected response: %d %s", r.Code, r.Body.String())
	}
}

func TestMonitorConcurrentClientState(t *testing.T) {
	b := &Broker{}
	c := &client{}
	b.clients.Store("client", c)
	router := monitorRouter(b)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			c.stateMu.Lock()
			c.lastMsgTime = int64(i)
			c.info.willMsg = &packets.PublishPacket{TopicName: "will", Payload: []byte("data")}
			c.stateMu.Unlock()
		}
	}()
	for i := 0; i < 100; i++ {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/connections", nil))
		if w.Code != 200 {
			t.Errorf("status: %d", w.Code)
		}
	}
	wg.Wait()
}

func TestHTTPMonitorListenFailureLogged(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)
	old := log
	log = zap.New(core)
	defer func() { log = old }()
	InitHTTPMoniter(&Broker{config: &Config{HTTPPort: "invalid-port"}})
	if logs.FilterMessage("monitor HTTP listen failed").Len() != 1 {
		t.Fatal("monitor listen failure was not logged")
	}
}
