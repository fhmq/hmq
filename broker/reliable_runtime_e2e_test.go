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

func TestReliableRuntimeMQTTEndToEnd(t *testing.T) {
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
	if err = os.WriteFile(caPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	config := func(id string) Config {
		k, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if e != nil {
			t.Fatal(e)
		}
		u, _ := url.Parse("urn:hmq:node:" + id)
		leaf := &x509.Certificate{SerialNumber: big.NewInt(2), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: ca.ExtKeyUsage, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{u}}
		cert, e := x509.CreateCertificate(rand.Reader, leaf, ca, &k.PublicKey, key)
		if e != nil {
			t.Fatal(e)
		}
		priv, e := x509.MarshalECPrivateKey(k)
		if e != nil {
			t.Fatal(e)
		}
		cp, kp := filepath.Join(dir, id+".pem"), filepath.Join(dir, id+"-key.pem")
		if e = os.WriteFile(cp, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert}), 0600); e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(kp, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: priv}), 0600); e != nil {
			t.Fatal(e)
		}
		return Config{Host: "127.0.0.1", Port: "0", Cluster: RouteInfo{NodeID: id, ClusterID: "test", Secret: "secret", Listen: "127.0.0.1:0", TLS: ClusterTLSInfo{CertFile: cp, KeyFile: kp, CAFile: caPath}, Reliable: ReliableConfig{Enabled: true, MemberIDs: []string{"a", "b"}, EventLogPath: filepath.Join(dir, id+"-events"), InboxLogPath: filepath.Join(dir, id+"-inbox")}}}
	}
	caConfig, cbConfig := config("a"), config("b")
	a, err := NewBroker(&caConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	cbConfig.Cluster.Seeds = []string{a.clusterNode.Addr()}
	b, err := NewBroker(&cbConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	a.Start()
	b.Start()
	deadline := time.Now().Add(10 * time.Second)
	connect := func(b *Broker, id string) mqtt.Client {
		var addr string
		for addr == "" && time.Now().Before(deadline) {
			b.lifecycleMu.Lock()
			for l := range b.listeners {
				addr = l.Addr().String()
			}
			b.lifecycleMu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
		if addr == "" {
			t.Fatal("no MQTT listener")
		}
		c := mqtt.NewClient(mqtt.NewClientOptions().AddBroker("tcp://" + addr).SetClientID(id).SetAutoReconnect(false))
		tok := c.Connect()
		if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
			t.Fatalf("connect: %v", tok.Error())
		}
		t.Cleanup(func() { c.Disconnect(100) })
		return c
	}
	pub, sub := connect(a, "reliable-pub"), connect(b, "reliable-sub")
	got := make(chan string, 1)
	tok := sub.Subscribe("reliable/test", 1, func(_ mqtt.Client, m mqtt.Message) { got <- string(m.Payload()) })
	if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		t.Fatalf("subscribe: %v", tok.Error())
	}
	tok = pub.Publish("reliable/test", 1, false, "hello")
	if !tok.WaitTimeout(3*time.Second) || tok.Error() != nil {
		t.Fatalf("publish: %v", tok.Error())
	}
	select {
	case value := <-got:
		if value != "hello" {
			t.Fatal(value)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("delivery timed out")
	}
	if err := a.reliableRuntime.events.Close(); err != nil {
		t.Fatal(err)
	}
	tok = pub.Publish("reliable/test", 1, false, "must-fail")
	tok.WaitTimeout(3 * time.Second)
	deadline = time.Now().Add(3 * time.Second)
	for pub.IsConnected() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if pub.IsConnected() {
		t.Fatal("publisher remained connected after durable enqueue failure")
	}
}
