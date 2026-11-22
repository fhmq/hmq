package broker

import (
	stdjson "encoding/json"
	"net"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	mqtt5 "github.com/eclipse/paho.golang/packets"
	"github.com/fhmq/hmq/v4/broker/packets"
	"github.com/fhmq/hmq/v4/broker/sessionstore"
)

func TestSessionQuotaMQTT(t *testing.T) {
	quota := sessionstore.Limits{MaxPendingMessages: 1}
	t.Run("standalone", func(t *testing.T) {
		cfg := Config{Worker: 2, ExperimentalSessionLogPath: filepath.Join(t.TempDir(), "sessions.wal"), SessionQuota: quota}
		open := func() *Broker {
			b, err := NewBroker(&cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(b.Close)
			return b
		}
		b := open()
		exerciseFullSession(t, b, b, func() *Broker {
			b.Close()
			// Reopening with different limits must not change WAL replay.
			cfg.SessionQuota.MaxPendingBytes = 1024
			return open()
		})
	})
	t.Run("quorum", func(t *testing.T) {
		cfgs := quorumTestConfigs(t)
		var nodes []*Broker
		for i := range cfgs {
			cfgs[i].Worker = 2
			cfgs[i].SessionQuota = quota
			cfgs[i].Cluster.Quorum.SessionPersistence = true
			for j := range cfgs {
				if j != i {
					cfgs[i].Cluster.Seeds = append(cfgs[i].Cluster.Seeds, cfgs[j].Cluster.Listen)
				}
			}
			b, err := NewBroker(&cfgs[i])
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(b.Close)
			nodes = append(nodes, b)
		}
		leader := quorumLeader(t, nodes)
		var survivors []*Broker
		for i, b := range nodes {
			if i != leader {
				survivors = append(survivors, b)
			}
		}
		exerciseFullSession(t, nodes[leader], survivors[0], func() *Broker {
			nodes[leader].Close()
			quorumLeader(t, survivors)
			return survivors[1]
		})
	})
}

func exerciseFullSession(t *testing.T, subBroker, pubBroker *Broker, recoverBroker func() *Broker) {
	t.Helper()
	for id, filter := range map[string]string{"full": "limit/+", "spare": "limit/both"} {
		c, done := logMQTTConnect(t, subBroker, id, false)
		legacyLoadSubscribe(t, c, filter, 1)
		c.Close()
		<-done
	}
	write := func(c net.Conn, p packets.ControlPacket) {
		t.Helper()
		if err := p.Write(c); err != nil {
			t.Fatal(err)
		}
	}
	read := func(c net.Conn) packets.ControlPacket {
		t.Helper()
		p, err := packets.ReadPacket(c)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	noACK := func(c net.Conn, done <-chan error) {
		t.Helper()
		if p, err := packets.ReadPacket(c); err == nil {
			t.Fatalf("full queue returned %T", p)
		} else if ne, ok := err.(net.Error); ok && ne.Timeout() {
			t.Fatalf("publisher was not disconnected: %v", err)
		}
		<-done
	}

	pub, done := logMQTTConnect(t, pubBroker, "sender", false)
	makePublish := func(qos byte, topic, value string) *packets.PublishPacket {
		p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		p.Qos, p.MessageID, p.TopicName, p.Payload = qos, 1, topic, []byte(value)
		return p
	}
	write(pub, makePublish(2, "limit/both", "second"))
	if p, ok := read(pub).(*packets.PubrecPacket); !ok || p.MessageID != 1 {
		t.Fatalf("expected PUBREC, got %#v", p)
	}
	other, otherDone := logMQTTConnect(t, pubBroker, "other-sender", false)
	if err := legacyLoadPublish(other, makePublish(1, "limit/fill", "first")); err != nil {
		t.Fatal(err)
	}
	write(other, makePublish(1, "limit/both", "rejected"))
	noACK(other, otherDone)
	rel := packets.NewControlPacket(packets.Pubrel).(*packets.PubrelPacket)
	rel.MessageID = 1
	write(pub, rel)
	noACK(pub, done)

	w := httptest.NewRecorder()
	monitorRouter(pubBroker).ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/capacity", nil))
	var cap brokerCapacity
	if err := stdjson.Unmarshal(w.Body.Bytes(), &cap); w.Code != 200 || err != nil ||
		cap.PersistentSessions == nil || cap.PersistentSessions.Queued.Messages != 1 || cap.PersistentSessions.Queued.Bytes == 0 ||
		cap.PersistentSessions.InboundQoS2.Messages != 1 || cap.OldestQueuedAgeSeconds == nil || cap.QuotaRejections != 2 {
		t.Fatalf("capacity: %s (error %v)", w.Body.String(), err)
	}
	if pubBroker.quorumEnabled() && (cap.Raft == nil || cap.Raft.Entries == 0 || cap.Raft.Bytes == 0 || cap.Raft.MaxBytes <= cap.Raft.Bytes) {
		t.Fatalf("Raft capacity: %#v", cap.Raft)
	}

	b := recoverBroker()
	drain := func(id, payload string) {
		c, done := logMQTTConnect(t, b, id, true)
		p, ok := read(c).(*packets.PublishPacket)
		if !ok || string(p.Payload) != payload {
			t.Fatalf("%s: expected %q, got %#v", id, payload, p)
		}
		ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		ack.MessageID = p.MessageID
		write(c, ack)
		write(c, packets.NewControlPacket(packets.Pingreq))
		if p := read(c); p == nil {
			t.Fatal("missing PINGRESP")
		} else if _, ok := p.(*packets.PingrespPacket); !ok {
			t.Fatalf("unexpected extra delivery: %#v", p)
		}
		c.Close()
		<-done
	}
	drain("full", "first")
	pub, done = logMQTTConnect(t, b, "sender", true)
	for range 2 {
		write(pub, rel)
		if p, ok := read(pub).(*packets.PubcompPacket); !ok || p.MessageID != 1 {
			t.Fatalf("expected resumed PUBCOMP, got %#v", p)
		}
	}
	pub.Close()
	<-done
	drain("full", "second")
	drain("spare", "second")
}

func TestSessionQuotaMQTT5NegativeACK(t *testing.T) {
	addr := mqtt5ITBroker(t, &Config{Worker: 2, ExperimentalSessionLogPath: filepath.Join(t.TempDir(), "sessions.wal"),
		SessionQuota: sessionstore.Limits{MaxPendingMessages: 1}})
	sub := mqtt5ITClient(t, addr, "quota-sub", func(p *mqtt5.Connect) {
		expiry := ^uint32(0)
		p.Properties.SessionExpiryInterval = &expiry
	})
	mqtt5ITSubscribe(t, sub, 1, mqtt5.SubOptions{Topic: "limit", QoS: 1}, nil)
	sub.Close()
	pub := mqtt5ITClient(t, addr, "quota-pub", nil)
	mqtt5ITPublish(t, pub, &mqtt5.Publish{Topic: "limit", QoS: 1, PacketID: 1, Payload: []byte("accepted")})
	mqtt5ITWrite(t, pub, &mqtt5.Publish{Topic: "limit", QoS: 1, PacketID: 2, Payload: []byte("rejected"), Properties: &mqtt5.Properties{}})
	ack := mqtt5ITRead[*mqtt5.Puback](t, pub)
	if ack.PacketID != 2 || ack.ReasonCode != 0x97 {
		t.Fatalf("quota must be a negative ACK: %#v", ack)
	}
	source := mqtt5ITClient(t, addr, "quota-qos2", func(p *mqtt5.Connect) {
		expiry := ^uint32(0)
		p.Properties.SessionExpiryInterval = &expiry
	})
	mqtt5ITWrite(t, source, &mqtt5.Publish{Topic: "limit", QoS: 2, PacketID: 3, Payload: []byte("rejected"), Properties: &mqtt5.Properties{}})
	rec := mqtt5ITRead[*mqtt5.Pubrec](t, source)
	if rec.PacketID != 3 || rec.ReasonCode != 0x97 {
		t.Fatalf("quota must reject before accepting QoS 2 ownership: %#v", rec)
	}
	_ = pub.SetDeadline(time.Now().Add(2 * time.Second))
	mqtt5ITPing(t, pub)
}
