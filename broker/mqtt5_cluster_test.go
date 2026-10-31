package broker

import (
	"bytes"
	stdjson "encoding/json"
	"reflect"
	"testing"
	"time"

	mqtt5 "github.com/eclipse/paho.golang/packets"
)

func TestMQTT5QuorumFollowerMetadata(t *testing.T) {
	configs := quorumTestConfigs(t)
	for i := range configs {
		for j := range configs {
			if i != j {
				configs[i].Cluster.Seeds = append(configs[i].Cluster.Seeds, configs[j].Cluster.Listen)
			}
		}
	}
	nodes := make([]*Broker, len(configs))
	for i := range configs {
		configs[i].Host, configs[i].Port = "127.0.0.1", "0"
		b, err := NewBroker(&configs[i])
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = b
		t.Cleanup(b.Close)
		b.Start()
	}
	leader := quorumLeader(t, nodes)
	for i, b := range nodes {
		deadline := time.Now().Add(8 * time.Second)
		for len(b.clusterNode.Peers()) != len(nodes)-1 {
			if time.Now().After(deadline) {
				t.Fatalf("node %d cluster peers incomplete: %v", i, b.clusterNode.Peers())
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	address := func(b *Broker) string {
		t.Helper()
		for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
			b.lifecycleMu.Lock()
			var addr string
			for listener := range b.listeners {
				addr = listener.Addr().String()
			}
			b.lifecycleMu.Unlock()
			if addr != "" {
				return addr
			}
			time.Sleep(time.Millisecond)
		}
		t.Fatal("MQTT listener did not start")
		return ""
	}
	checkConnack := func(ack *mqtt5.Connack) {
		t.Helper()
		if ack.ReasonCode != 0 || ack.SessionPresent || ack.Properties == nil {
			t.Fatalf("quorum CONNACK: %#v", ack)
		}
		p := ack.Properties
		if p.MaximumQOS == nil || *p.MaximumQOS != 1 ||
			p.RetainAvailable == nil || *p.RetainAvailable != 0 ||
			p.SharedSubAvailable == nil || *p.SharedSubAvailable != 0 {
			t.Fatalf("quorum capability flags: %#v", p)
		}
	}
	// Use distinct followers so neither local routing nor a leader connection can satisfy delivery.
	publisherNode, subscriberNode := (leader+1)%len(nodes), (leader+2)%len(nodes)
	sub, ack := mqtt5ITConnect(t, address(nodes[subscriberNode]), "mqtt5-quorum-sub", nil)
	checkConnack(ack)
	mqtt5ITSubscribe(t, sub, 1, mqtt5.SubOptions{Topic: "mqtt5/quorum", QoS: 1}, nil)
	pub, ack := mqtt5ITConnect(t, address(nodes[publisherNode]), "mqtt5-quorum-pub", nil)
	checkConnack(ack)
	properties := &mqtt5.Properties{
		ContentType:     "application/octet-stream",
		ResponseTopic:   "mqtt5/response",
		CorrelationData: []byte{0, 255, 1},
		User:            []mqtt5.User{{Key: "key", Value: "first"}, {Key: "key", Value: "second"}},
	}
	payload := []byte("committed metadata")
	// This helper requires a successful PUBACK with the original packet identifier.
	mqtt5ITPublish(t, pub, &mqtt5.Publish{
		Topic: "mqtt5/quorum", Payload: payload, QoS: 1, PacketID: 42, Properties: properties,
	})
	entries, err := nodes[leader].quorumRuntime.node.Entries(0, 10)
	if err != nil || len(entries) != 1 || entries[0].Index == 0 {
		t.Fatalf("PUBACK without committed publication: entries=%v err=%v", entries, err)
	}
	for i, b := range nodes {
		quorumWaitCursor(t, b, entries[0].Index)
		replicated, err := b.quorumRuntime.node.Entries(0, 10)
		if err != nil || len(replicated) != 1 || !bytes.Equal(replicated[0].Payload, entries[0].Payload) {
			t.Fatalf("node %d replicated publication: entries=%v err=%v", i, replicated, err)
		}
	}
	var committed clusterPublish
	if err := stdjson.Unmarshal(entries[0].Payload, &committed); err != nil {
		t.Fatal(err)
	}
	if committed.Metadata == nil || committed.Metadata.Origin != "mqtt5-quorum-pub" ||
		!reflect.DeepEqual(committed.Metadata.Properties, properties) {
		t.Fatalf("committed metadata: %#v", committed.Metadata)
	}
	got := mqtt5ITRead[*mqtt5.Publish](t, sub)
	if got.Topic != "mqtt5/quorum" || got.QoS != 1 || got.PacketID == 0 || got.Retain ||
		!bytes.Equal(got.Payload, payload) || !reflect.DeepEqual(got.Properties, properties) {
		t.Fatalf("cross-node MQTT 5 delivery: %#v", got)
	}
	mqtt5ITAck(t, sub, got)
	mqtt5ITPing(t, sub)
	mqtt5ITPing(t, pub)
}
