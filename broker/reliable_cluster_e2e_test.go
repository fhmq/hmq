package broker

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

// reliableClusterHarness is the adapter contract for the pending runtime.
// Restart preserves durable storage and re-establishes the test subscription;
// Partition isolates one node without stopping its MQTT listener. Deliveries
// observes application deliveries.
type reliableClusterHarness interface {
	Partition(node string, isolated bool)
	Restart(node string)
	Publish(node, payload string)
	Deliveries(node string) <-chan string
	Close()
}

type realReliableCluster struct {
	t          *testing.T
	configs    map[string]Config
	nodes      map[string]*Broker
	clients    map[string]mqtt.Client
	deliveries map[string]chan string
	links      []*mqttFaultLink
}

func newReliableClusterHarness(t *testing.T) reliableClusterHarness {
	t.Helper()
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	h := &realReliableCluster{t: t, configs: make(map[string]Config), nodes: make(map[string]*Broker), clients: make(map[string]mqtt.Client), deliveries: make(map[string]chan string)}
	t.Cleanup(h.Close)
	for _, id := range []string{"a", "b", "c"} {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		u, _ := url.Parse("urn:hmq:node:" + id)
		leaf := &x509.Certificate{SerialNumber: big.NewInt(2), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: ca.ExtKeyUsage, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{u}}
		cert, err := x509.CreateCertificate(rand.Reader, leaf, ca, &k.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		priv, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			t.Fatal(err)
		}
		cp, kp := filepath.Join(dir, id+".pem"), filepath.Join(dir, id+"-key.pem")
		if err := os.WriteFile(cp, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert}), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(kp, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: priv}), 0600); err != nil {
			t.Fatal(err)
		}
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := l.Addr().String()
		l.Close()
		h.configs[id] = Config{Host: "127.0.0.1", Port: "0", Cluster: RouteInfo{NodeID: id, ClusterID: "test", Secret: "secret", Listen: addr, TLS: ClusterTLSInfo{CertFile: cp, KeyFile: kp, CAFile: caPath}, Reliable: ReliableConfig{Enabled: true, MemberIDs: []string{"a", "b", "c"}, EventLogPath: filepath.Join(dir, id+"-events"), InboxLogPath: filepath.Join(dir, id+"-inbox")}}}
		h.deliveries[id] = make(chan string, 32)
	}
	a, b := h.configs["a"], h.configs["b"]
	b.Cluster.Seeds = []string{a.Cluster.Listen}
	h.configs["b"] = b
	for _, target := range []string{a.Cluster.Listen, b.Cluster.Listen} {
		h.links = append(h.links, newMQTTFaultLink(t, target))
	}
	c := h.configs["c"]
	c.Cluster.Seeds = []string{h.links[0].listener.Addr().String(), h.links[1].listener.Addr().String()}
	h.configs["c"] = c
	for _, id := range []string{"a", "b", "c"} {
		h.start(id)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(h.nodes["a"].clusterNode.Peers()) == 2 && len(h.nodes["b"].clusterNode.Peers()) == 2 && len(h.nodes["c"].clusterNode.Peers()) == 2 {
			return h
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("cluster did not form a full mesh")
	return nil
}

func (h *realReliableCluster) start(id string) {
	h.t.Helper()
	cfg := h.configs[id]
	b, err := NewBroker(&cfg)
	if err != nil {
		h.t.Fatal(err)
	}
	h.nodes[id] = b
	b.Start()
	var addr string
	deadline := time.Now().Add(5 * time.Second)
	for addr == "" && time.Now().Before(deadline) {
		b.lifecycleMu.Lock()
		for l := range b.listeners {
			addr = l.Addr().String()
		}
		b.lifecycleMu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
	if addr == "" {
		h.t.Fatal("MQTT listener did not start")
	}
	c := mqtt.NewClient(mqtt.NewClientOptions().AddBroker("tcp://" + addr).SetClientID("e2e-" + id).SetAutoReconnect(false))
	tok := c.Connect()
	if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		h.t.Fatalf("connect %s: %v", id, tok.Error())
	}
	h.clients[id] = c
	tok = c.Subscribe("reliable/e2e", 1, func(_ mqtt.Client, m mqtt.Message) { h.deliveries[id] <- string(m.Payload()) })
	if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		h.t.Fatalf("subscribe %s: %v", id, tok.Error())
	}
}

func (h *realReliableCluster) Partition(node string, isolated bool) {
	if node != "c" {
		h.t.Fatalf("unsupported partition %s", node)
	}
	for _, link := range h.links {
		link.setBlocked(isolated)
	}
}
func (h *realReliableCluster) Restart(node string) {
	h.clients[node].Disconnect(100)
	h.nodes[node].Close()
	delete(h.clients, node)
	delete(h.nodes, node)
	h.start(node)
}
func (h *realReliableCluster) Publish(node, payload string) {
	tok := h.clients[node].Publish("reliable/e2e", 1, false, payload)
	if !tok.WaitTimeout(5*time.Second) || tok.Error() != nil {
		h.t.Fatalf("publish: %v", tok.Error())
	}
}
func (h *realReliableCluster) Deliveries(node string) <-chan string { return h.deliveries[node] }
func (h *realReliableCluster) Close() {
	for id, c := range h.clients {
		c.Disconnect(100)
		delete(h.clients, id)
	}
	for id, b := range h.nodes {
		b.Close()
		delete(h.nodes, id)
	}
}

func reliableExpectDelivery(t *testing.T, deliveries <-chan string, want string) {
	t.Helper()
	select {
	case got, ok := <-deliveries:
		if !ok || got != want {
			t.Fatalf("delivery: got %q (open=%v), want %q", got, ok, want)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for delivery %q", want)
	}
}

func reliableExpectQuiet(t *testing.T, deliveries <-chan string) {
	t.Helper()
	select {
	case got, ok := <-deliveries:
		if ok {
			t.Fatalf("unexpected duplicate delivery %q", got)
		}
	case <-time.After(3 * time.Second):
	}
}

func TestReliableClusterPartitionRecoveryAndDedup(t *testing.T) {
	h := newReliableClusterHarness(t)
	defer h.Close()
	h.Partition("c", true)
	h.Publish("a", "partitioned")
	reliableExpectDelivery(t, h.Deliveries("b"), "partitioned")
	reliableExpectQuiet(t, h.Deliveries("c"))
	h.Partition("c", false)
	reliableExpectDelivery(t, h.Deliveries("c"), "partitioned")
	reliableExpectQuiet(t, h.Deliveries("b"))
	reliableExpectQuiet(t, h.Deliveries("c"))
}

func TestReliableClusterRestartRecovery(t *testing.T) {
	h := newReliableClusterHarness(t)
	defer h.Close()
	h.Partition("c", true)
	h.Publish("a", "before-restart")
	reliableExpectDelivery(t, h.Deliveries("b"), "before-restart")
	h.Restart("a")
	h.Restart("c")
	h.Partition("c", false)
	reliableExpectDelivery(t, h.Deliveries("c"), "before-restart")
	reliableExpectQuiet(t, h.Deliveries("b"))
	reliableExpectQuiet(t, h.Deliveries("c"))
}

func TestReliableClusterCloseDuringPartition(t *testing.T) {
	h := newReliableClusterHarness(t)
	h.Partition("c", true)
	h.Publish("a", "pending-on-close")
	done := make(chan struct{})
	go func() { h.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("cluster close blocked with a disconnected peer")
	}
}
