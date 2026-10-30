package broker

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	stdjson "encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/v3/broker/storage"
	"github.com/hashicorp/raft"
)

func quorumTestAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	_ = l.Close()
	return address
}

func quorumTestConfigs(t *testing.T) []Config {
	t.Helper()
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	write := func(name, kind string, data []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: data}), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	caPath := write("ca.pem", "CERTIFICATE", der)
	peers := map[string]string{}
	for _, id := range []string{"a", "b", "c"} {
		peers[id] = quorumTestAddress(t)
	}
	var configs []Config
	for i, id := range []string{"a", "b", "c"} {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		u, _ := url.Parse("urn:hmq:node:" + id)
		cert := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, URIs: []*url.URL{u}}
		der, err := x509.CreateCertificate(rand.Reader, cert, ca, &k.PublicKey, key)
		if err != nil {
			t.Fatal(err)
		}
		priv, err := x509.MarshalECPrivateKey(k)
		if err != nil {
			t.Fatal(err)
		}
		configs = append(configs, Config{Cluster: RouteInfo{NodeID: id, ClusterID: "test", Secret: "secret", Listen: quorumTestAddress(t), TLS: ClusterTLSInfo{CertFile: write(id+".pem", "CERTIFICATE", der), KeyFile: write(id+".key", "EC PRIVATE KEY", priv), CAFile: caPath}, Quorum: QuorumConfig{Enabled: true, DataDir: filepath.Join(dir, id), Listen: peers[id], Peers: peers}}})
	}
	return configs
}

func TestQuorumConfigValidation(t *testing.T) {
	base := quorumTestConfigs(t)[0]
	for _, tc := range []struct {
		name string
		edit func(*Config)
	}{
		{"disabled", func(c *Config) { c.Cluster.Quorum.Enabled = false }},
		{"reliable", func(c *Config) { c.Cluster.Reliable.Enabled = true }},
		{"blank directory", func(c *Config) { c.Cluster.Quorum.DataDir = " " }},
		{"missing self", func(c *Config) { c.Cluster.Quorum.Peers = map[string]string{"b": "127.0.0.1:1234"} }},
		{"wrong self", func(c *Config) { c.Cluster.Quorum.Listen = "127.0.0.1:1234" }},
		{"retained overlap", func(c *Config) { c.RetainedLogPath = filepath.Join(c.Cluster.Quorum.DataDir, "retained") }},
		{"session overlap", func(c *Config) { c.ExperimentalSessionLogPath = filepath.Join(c.Cluster.Quorum.DataDir, "sessions") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.edit(&c)
			if err := validateQuorumConfig(&c); err == nil {
				t.Fatal("accepted invalid config")
			}
			if b, err := NewBroker(&c); err == nil {
				b.Close()
				t.Fatal("broker accepted invalid config")
			}
		})
	}
	if err := base.check(); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(base.Cluster.Quorum.DataDir, 0700); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(t.TempDir(), "alias")
	if err := os.Symlink(base.Cluster.Quorum.DataDir, alias); err != nil {
		t.Fatal(err)
	}
	base.RetainedLogPath = filepath.Join(alias, "retained")
	if err := validateQuorumConfig(&base); err == nil {
		t.Fatal("accepted symlink overlap")
	}
}

func TestQuorumInitializationFailureReleasesListener(t *testing.T) {
	c := quorumTestConfigs(t)[0]
	// An incomplete initialization must fail closed without leaking a listener.
	if err := os.MkdirAll(filepath.Join(c.Cluster.Quorum.DataDir, "raft.wal"), 0700); err != nil {
		t.Fatal(err)
	}
	if b, err := NewBroker(&c); err == nil {
		b.Close()
		t.Fatal("accepted directory as WAL")
	}
	l, err := net.Listen("tcp", c.Cluster.Quorum.Listen)
	if err != nil {
		t.Fatal("listener leaked:", err)
	}
	_ = l.Close()
}

func TestQuorumRejectsIncompleteStateAndCursorAhead(t *testing.T) {
	for _, missing := range []string{"raft.wal", "delivery.wal", "empty-raft", "empty-cursor", "cursor-ahead"} {
		t.Run(missing, func(t *testing.T) {
			c := quorumTestConfigs(t)[0]
			b, err := NewBroker(&c)
			if err != nil {
				t.Fatal(err)
			}
			b.Close()
			if missing == "cursor-ahead" {
				l, err := storage.Open(filepath.Join(c.Cluster.Quorum.DataDir, "delivery.wal"))
				if err != nil {
					t.Fatal(err)
				}
				data, _ := stdjson.Marshal(quorumCursorRecord{Version: 1, ID: c.Cluster.NodeID, Cluster: c.Cluster.ClusterID, Index: 1 << 60})
				if err := l.Append(data); err != nil {
					t.Fatal(err)
				}
				_ = l.Close()
			} else if missing == "empty-raft" || missing == "empty-cursor" {
				name := "raft.wal"
				if missing == "empty-cursor" {
					name = "delivery.wal"
				}
				if err := os.Truncate(filepath.Join(c.Cluster.Quorum.DataDir, name), 0); err != nil {
					t.Fatal(err)
				}
			} else if err := os.Remove(filepath.Join(c.Cluster.Quorum.DataDir, missing)); err != nil {
				t.Fatal(err)
			}
			if b, err := NewBroker(&c); err == nil {
				b.Close()
				t.Fatal("accepted incomplete or impossible cursor state")
			}
			l, err := net.Listen("tcp", c.Cluster.Quorum.Listen)
			if err != nil {
				t.Fatal("listener leaked:", err)
			}
			_ = l.Close()
		})
	}
}

func TestQuorumRejectsCrossClusterDataDirectory(t *testing.T) {
	c := quorumTestConfigs(t)[0]
	b, err := NewBroker(&c)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	c.Cluster.ClusterID = "other-cluster"
	if b, err := NewBroker(&c); err == nil {
		b.Close()
		t.Fatal("accepted cross-cluster data directory reuse")
	}
	l, err := net.Listen("tcp", c.Cluster.Quorum.Listen)
	if err != nil {
		t.Fatal("listener leaked:", err)
	}
	_ = l.Close()
}

func TestQuorumNetworkLifecycle(t *testing.T) {
	configs := quorumTestConfigs(t)
	var nodes []*Broker
	for i := range configs {
		configs[i].Host, configs[i].Port = "127.0.0.1", "0"
		b, err := NewBroker(&configs[i])
		if err != nil {
			t.Fatal(err)
		}
		nodes = append(nodes, b)
		t.Cleanup(b.Close)
		b.Start()
	}
	var leader *Broker
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		for _, b := range nodes {
			if b.quorumRuntime.node.State() == raft.Leader {
				leader = b
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no network quorum leader")
	}
	if err := leader.PublishMessage(&packets.PublishPacket{TopicName: "test", Payload: []byte("committed")}); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for _, b := range nodes {
		for _, qos := range []byte{0, 1, 2} {
			quorumAssertNoAck(t, b, qos)
		}
		for {
			entries, err := b.quorumRuntime.node.Entries(0, 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) == 1 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("replica did not apply commit")
			}
			time.Sleep(20 * time.Millisecond)
		}
		p := &packets.PublishPacket{TopicName: "test", Payload: []byte("rejected")}
		p.Retain = true
		for _, publish := range []func() error{func() error { return b.PublishMessage(p) }, func() error { return b.PublishMessageByClientId(p, "missing") }, func() error { return b.deliverClusterMessage([]byte(`{"Topic":"test"}`)) }} {
			if !errors.Is(publish(), ErrQuorumRoutingUnavailable) {
				t.Fatal("publication not rejected")
			}
		}
		w := httptest.NewRecorder()
		monitorRouter(b).ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/cluster/quorum", nil))
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"routing":"quorum_qos01_nonretained"`) {
			t.Fatal(w.Body.String())
		}
	}
	for _, b := range nodes {
		b.Close()
		b.Close()
		if b.quorumRuntime.health().State != "closed" {
			t.Fatal("not closed")
		}
		l, err := net.Listen("tcp", b.config.Cluster.Quorum.Listen)
		if err != nil {
			t.Fatal(err)
		}
		_ = l.Close()
	}
}

func quorumAssertNoAck(t *testing.T, b *Broker, qos byte) {
	t.Helper()
	var addr string
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		b.lifecycleMu.Lock()
		for l := range b.listeners {
			addr = l.Addr().String()
		}
		b.lifecycleMu.Unlock()
		if addr != "" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if addr == "" {
		t.Fatal("MQTT listener did not start")
	}
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	c := packets.NewControlPacket(packets.Connect).(*packets.ConnectPacket)
	c.ProtocolName, c.ProtocolVersion, c.CleanSession, c.ClientIdentifier = "MQTT", 4, true, fmt.Sprintf("quorum-%d", qos)
	if err := c.Write(conn); err != nil {
		t.Fatal(err)
	}
	p, err := packets.ReadPacket(conn)
	if err != nil {
		t.Fatal(err)
	}
	if ack, ok := p.(*packets.ConnackPacket); !ok || ack.ReturnCode != 0 {
		t.Fatalf("CONNACK: %#v", p)
	}
	pub := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	pub.TopicName, pub.Payload, pub.MessageID, pub.Qos, pub.Retain = "test", []byte("must reject"), 42, qos, true
	if err := pub.Write(conn); err != nil {
		t.Fatal(err)
	}
	p, err = packets.ReadPacket(conn)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected disconnect without ACK, got %#v, %v", p, err)
	}
}
