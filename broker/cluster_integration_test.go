package broker

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/fhmq/hmq/v4/broker/packets"
)

type mqttFaultLink struct {
	listener net.Listener
	target   string
	mu       sync.Mutex
	blocked  bool
	conns    map[net.Conn]bool
	wg       sync.WaitGroup
}

func newMQTTFaultLink(t *testing.T, target string) *mqttFaultLink {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &mqttFaultLink{listener: l, target: target, conns: make(map[net.Conn]bool)}
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			f.mu.Lock()
			if f.blocked {
				f.mu.Unlock()
				c.Close()
				continue
			}
			f.conns[c] = true
			f.wg.Add(1)
			f.mu.Unlock()
			go func() {
				defer f.wg.Done()
				defer func() { f.mu.Lock(); delete(f.conns, c); f.mu.Unlock(); c.Close() }()
				u, err := net.DialTimeout("tcp", f.target, time.Second)
				if err != nil {
					return
				}
				defer u.Close()
				go func() { io.Copy(u, c); u.Close() }()
				io.Copy(c, u)
			}()
		}
	}()
	t.Cleanup(func() { f.setBlocked(true); l.Close(); f.wg.Wait() })
	return f
}

func (f *mqttFaultLink) setBlocked(blocked bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocked = blocked
	if blocked {
		for c := range f.conns {
			c.Close()
		}
	}
}

func TestClusterRetainAndClose(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile := filepath.Join(dir, "cert.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	nodeCert := func(id string) (string, string) {
		nodeKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		keyDER, err := x509.MarshalECPrivateKey(nodeKey)
		if err != nil {
			t.Fatal(err)
		}
		u, err := url.Parse("urn:hmq:node:" + id)
		if err != nil {
			t.Fatal(err)
		}
		leaf := &x509.Certificate{SerialNumber: big.NewInt(2), NotBefore: tmpl.NotBefore, NotAfter: tmpl.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: tmpl.ExtKeyUsage, IPAddresses: tmpl.IPAddresses, URIs: []*url.URL{u}}
		data, err := x509.CreateCertificate(rand.Reader, leaf, tmpl, &nodeKey.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, id+".pem")
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: data}), 0600); err != nil {
			t.Fatal(err)
		}
		keyPath := filepath.Join(dir, id+"-key.pem")
		if err := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0600); err != nil {
			t.Fatal(err)
		}
		return path, keyPath
	}
	aCert, aKey := nodeCert("a")
	cfg := RouteInfo{NodeID: "a", ClusterID: "test", Secret: "secret", Listen: "127.0.0.1:0", TLS: ClusterTLSInfo{CertFile: aCert, KeyFile: aKey, CAFile: certFile}}
	a, err := NewBroker(&Config{Cluster: cfg, Host: "127.0.0.1", Port: "0"})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	cfg.NodeID = "b"
	cfg.TLS.CertFile, cfg.TLS.KeyFile = nodeCert("b")
	cfg.Seeds = []string{a.clusterNode.Addr()}
	b, err := NewBroker(&Config{Cluster: cfg, Host: "127.0.0.1", Port: "0"})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	cfg.NodeID = "c"
	cfg.TLS.CertFile, cfg.TLS.KeyFile = nodeCert("c")
	ac, bc := newMQTTFaultLink(t, a.clusterNode.Addr()), newMQTTFaultLink(t, b.clusterNode.Addr())
	cfg.Seeds = []string{ac.listener.Addr().String(), bc.listener.Addr().String()}
	c, err := NewBroker(&Config{Cluster: cfg, Host: "127.0.0.1", Port: "0"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { c.Close() }()
	a.Start()
	b.Start()
	c.Start()
	deadline := time.Now().Add(5 * time.Second)
	for len(a.clusterNode.Peers()) != 2 || len(b.clusterNode.Peers()) != 2 || len(c.clusterNode.Peers()) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("cluster peers did not connect")
		}
		time.Sleep(10 * time.Millisecond)
	}
	server := httptest.NewServer(a.trackHTTP(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })))
	resp, err := http.Get(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	server.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("HTTP status %d", resp.StatusCode)
	}
	connect := func(broker *Broker, id string) mqtt.Client {
		var addr string
		for addr == "" {
			broker.lifecycleMu.Lock()
			for l := range broker.listeners {
				addr = l.Addr().String()
			}
			broker.lifecycleMu.Unlock()
			if time.Now().After(deadline) {
				t.Fatal("MQTT listener did not start")
			}
			time.Sleep(time.Millisecond)
		}
		client := mqtt.NewClient(mqtt.NewClientOptions().AddBroker("tcp://" + addr).SetClientID(id).SetAutoReconnect(false))
		token := client.Connect()
		if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
			t.Fatalf("MQTT connect: %v", token.Error())
		}
		t.Cleanup(func() { client.Disconnect(100) })
		return client
	}
	ca, cb := connect(a, "cluster-a"), connect(b, "cluster-b")
	cc := connect(c, "cluster-c")
	legacyTopic := make(chan string, 1)
	token := cb.Subscribe("broker000100101info", 0, func(_ mqtt.Client, m mqtt.Message) { legacyTopic <- string(m.Payload()) })
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		t.Fatalf("subscribe legacy topic: %v", token.Error())
	}
	token = ca.Publish("broker000100101info", 0, false, "ordinary publication")
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		t.Fatalf("publish legacy topic: %v", token.Error())
	}
	select {
	case got := <-legacyTopic:
		if got != "ordinary publication" {
			t.Fatalf("unexpected legacy topic payload: %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("legacy topic was not delivered as an ordinary publication")
	}
	third := make(chan string, 8)
	tok := cc.Subscribe("mqtt/cluster", 0, func(_ mqtt.Client, m mqtt.Message) { third <- string(m.Payload()) })
	if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe c: %v", tok.Error())
	}
	local, remote := make(chan string, 4), make(chan string, 4)
	for _, sub := range []struct {
		client mqtt.Client
		ch     chan string
	}{{ca, local}, {cb, remote}} {
		token := sub.client.Subscribe("mqtt/cluster", 0, func(_ mqtt.Client, m mqtt.Message) { sub.ch <- string(m.Payload()) })
		if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
			t.Fatalf("subscribe: %v", token.Error())
		}
	}
	token = ca.Publish("mqtt/cluster", 0, true, "mqtt-message")
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		t.Fatalf("publish: %v", token.Error())
	}
	for _, ch := range []chan string{local, remote, third} {
		select {
		case got := <-ch:
			if got != "mqtt-message" {
				t.Fatalf("unexpected payload %q", got)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("MQTT publication not received")
		}
	}
	select {
	case got := <-local:
		t.Fatalf("looped local publication: %q", got)
	case got := <-remote:
		t.Fatalf("duplicate remote publication: %q", got)
	case got := <-third:
		t.Fatalf("duplicate third publication: %q", got)
	case <-time.After(300 * time.Millisecond):
	}
	ac.setBlocked(true)
	bc.setBlocked(true)
	deadline = time.Now().Add(5 * time.Second)
	for len(a.clusterNode.Peers()) != 1 || len(b.clusterNode.Peers()) != 1 || len(c.clusterNode.Peers()) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("partition did not take effect")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, p := range []struct {
		client mqtt.Client
		value  string
	}{{ca, "majority"}, {cc, "isolated"}} {
		tok := p.client.Publish("mqtt/conflict", 1, true, p.value)
		if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
			t.Fatalf("partition publish: %v", tok.Error())
		}
	}
	ac.setBlocked(false)
	bc.setBlocked(false)
	deadline = time.Now().Add(5 * time.Second)
	for len(a.clusterNode.Peers()) != 2 || len(b.clusterNode.Peers()) != 2 || len(c.clusterNode.Peers()) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("partition did not recover")
		}
		time.Sleep(10 * time.Millisecond)
	}
	for _, check := range []struct {
		client mqtt.Client
		want   string
	}{{ca, "majority"}, {cb, "majority"}, {cc, "isolated"}} {
		ch := make(chan string, 1)
		tok := check.client.Subscribe("mqtt/conflict", 1, func(_ mqtt.Client, m mqtt.Message) {
			if m.Retained() {
				ch <- string(m.Payload())
			}
		})
		if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
			t.Fatalf("conflict subscribe: %v", tok.Error())
		}
		select {
		case got := <-ch:
			if got != check.want {
				t.Fatalf("retained conflict: got %q want %q", got, check.want)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("missing retained conflict value")
		}
		tok = check.client.Unsubscribe("mqtt/conflict")
		if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
			t.Fatalf("unsubscribe: %v", tok.Error())
		}
	}
	updated := make(chan string, 1)
	tok = cc.Subscribe("mqtt/conflict", 1, func(_ mqtt.Client, m mqtt.Message) {
		if !m.Retained() {
			updated <- string(m.Payload())
		}
	})
	if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe for update: %v", tok.Error())
	}
	tok = cb.Publish("mqtt/conflict", 1, true, "after")
	if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		t.Fatalf("publish after partition: %v", tok.Error())
	}
	select {
	case got := <-updated:
		if got != "after" {
			t.Fatalf("update: %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("missing update after partition")
	}
	tok = cc.Unsubscribe("mqtt/conflict")
	if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		t.Fatalf("unsubscribe after update: %v", tok.Error())
	}
	retainedAfter := make(chan string, 1)
	tok = cc.Subscribe("mqtt/conflict", 1, func(_ mqtt.Client, m mqtt.Message) {
		if m.Retained() {
			retainedAfter <- string(m.Payload())
		}
	})
	if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe after update: %v", tok.Error())
	}
	select {
	case got := <-retainedAfter:
		if got != "after" {
			t.Fatalf("retained after update: %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("missing retained after update")
	}
	// Restart the third node at the same address; its seed connections must recover.
	cfg.Listen = c.clusterNode.Addr()
	cc.Disconnect(100)
	c.Close()
	deadline = time.Now().Add(5 * time.Second)
	for len(a.clusterNode.Peers()) != 1 || len(b.clusterNode.Peers()) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("third node did not disconnect")
		}
		time.Sleep(10 * time.Millisecond)
	}
	token = ca.Publish("mqtt/offline-retained", 1, true, "offline")
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		t.Fatalf("publish while node offline: %v", token.Error())
	}
	c, err = NewBroker(&Config{Cluster: cfg, Host: "127.0.0.1", Port: "0"})
	if err != nil {
		t.Fatal(err)
	}
	c.Start()
	deadline = time.Now().Add(5 * time.Second)
	for len(a.clusterNode.Peers()) != 2 || len(b.clusterNode.Peers()) != 2 || len(c.clusterNode.Peers()) != 2 {
		if time.Now().After(deadline) {
			t.Fatal("third node did not reconnect")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cc = connect(c, "cluster-c-reconnected")
	var missed []*packets.PublishPacket
	if err := c.topicsMgr.Retained([]byte("mqtt/offline-retained"), &missed); err != nil || len(missed) != 0 {
		t.Fatalf("offline retained should not be synchronized: count=%d err=%v", len(missed), err)
	}
	tok = cc.Subscribe("mqtt/reconnected", 0, func(_ mqtt.Client, m mqtt.Message) { third <- string(m.Payload()) })
	if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		t.Fatalf("resubscribe c: %v", tok.Error())
	}
	tok = cb.Publish("mqtt/reconnected", 0, false, "after-reconnect")
	if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		t.Fatalf("publish after reconnect: %v", tok.Error())
	}
	select {
	case got := <-third:
		if got != "after-reconnect" {
			t.Fatalf("unexpected payload %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reconnected node did not receive message")
	}
	select {
	case got := <-third:
		t.Fatalf("duplicate after reconnect: %q", got)
	case <-time.After(300 * time.Millisecond):
	}
	for qos := byte(0); qos <= 2; qos++ {
		messages := make(chan mqtt.Message, 4)
		topic := fmt.Sprintf("mqtt/qos/%d", qos)
		token = cb.Subscribe(topic, qos, func(_ mqtt.Client, m mqtt.Message) { messages <- m })
		if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
			t.Fatalf("subscribe qos %d: %v", qos, token.Error())
		}
		token = ca.Publish(topic, qos, false, "once")
		if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
			t.Fatalf("publish qos %d: %v", qos, token.Error())
		}
		select {
		case m := <-messages:
			if string(m.Payload()) != "once" || m.Qos() != qos || m.Retained() {
				t.Fatalf("qos %d: unexpected message qos=%d retained=%v payload=%q", qos, m.Qos(), m.Retained(), m.Payload())
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("qos %d: message lost", qos)
		}
		select {
		case <-messages:
			t.Fatalf("qos %d: duplicate message", qos)
		case <-time.After(200 * time.Millisecond):
		}
	}
	for pubQoS := byte(0); pubQoS <= 2; pubQoS++ {
		for subQoS := byte(0); subQoS <= 2; subQoS++ {
			topic := fmt.Sprintf("mqtt/matrix/%d/%d", pubQoS, subQoS)
			messages := make(chan mqtt.Message, 2)
			token = cc.Subscribe(topic, subQoS, func(_ mqtt.Client, m mqtt.Message) { messages <- m })
			if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
				t.Fatalf("subscribe %s: %v", topic, token.Error())
			}
			token = ca.Publish(topic, pubQoS, false, topic)
			if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
				t.Fatalf("publish %s: %v", topic, token.Error())
			}
			want := pubQoS
			if subQoS < want {
				want = subQoS
			}
			select {
			case m := <-messages:
				if m.Qos() != want || string(m.Payload()) != topic || m.Retained() {
					t.Fatalf("%s: qos=%d retained=%v payload=%q", topic, m.Qos(), m.Retained(), m.Payload())
				}
			case <-time.After(3 * time.Second):
				t.Fatalf("%s: message lost", topic)
			}
			select {
			case <-messages:
				t.Fatalf("%s: duplicate message", topic)
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
	retained := make(chan string, 1)
	token = cb.Subscribe("mqtt/cluster", 0, func(_ mqtt.Client, m mqtt.Message) {
		if m.Retained() {
			retained <- string(m.Payload())
		}
	})
	if !token.WaitTimeout(3*time.Second) || token.Error() != nil {
		t.Fatalf("retained subscribe: %v", token.Error())
	}
	select {
	case got := <-retained:
		if got != "mqtt-message" {
			t.Fatalf("unexpected retained payload %q", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("retained MQTT publication not received")
	}
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Payload, p.Retain = "test/topic", []byte("hello"), true
	a.PublishMessage(p)
	var localRetained []*packets.PublishPacket
	if err := a.topicsMgr.Retained([]byte(p.TopicName), &localRetained); err != nil || len(localRetained) != 1 {
		t.Fatalf("local retained publication: %v, count %d", err, len(localRetained))
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		a.broadcastClusterMessage(p)
		var retained []*packets.PublishPacket
		if err := b.topicsMgr.Retained([]byte(p.TopicName), &retained); err != nil {
			t.Fatal(err)
		}
		if len(retained) == 1 && string(retained[0].Payload) == "hello" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retained publication not received")
		}
		time.Sleep(20 * time.Millisecond)
	}
	p.Payload = nil
	a.broadcastClusterMessage(p)
	for {
		var retained []*packets.PublishPacket
		if err := b.topicsMgr.Retained([]byte(p.TopicName), &retained); err != nil {
			t.Fatal(err)
		}
		if len(retained) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("retained publication not deleted")
		}
		time.Sleep(20 * time.Millisecond)
	}
	done := make(chan struct{})
	go func() { b.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("close timed out")
	}
}
