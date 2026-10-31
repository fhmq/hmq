package broker

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/fhmq/hmq/v4/broker/packets"
	"github.com/fhmq/hmq/v4/broker/lib/topics"
)

type pausedSnapshot struct {
	topics.TopicsProvider
	subs    []interface{}
	ready   chan struct{}
	release chan struct{}
}

func (p *pausedSnapshot) Subscribers(_ []byte, _ byte, subs *[]interface{}, qoss *[]byte) error {
	*subs = append(*subs, p.subs...)
	for range p.subs {
		*qoss = append(*qoss, 1)
	}
	close(p.ready)
	<-p.release
	return nil
}

func TestPublishSnapshotRevalidation(t *testing.T) {
	for _, scenario := range []string{"clean", "unsubscribe", "resubscribe", "qos", "shared", "shared-fallback"} {
		t.Run(scenario, func(t *testing.T) {
			b, err := NewBroker(&Config{})
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			old := &client{info: info{clientID: "snapshot"}}
			stale := &subscription{client: old, topic: "snapshot/topic", qos: 1, generation: 1}
			if scenario == "shared" || scenario == "shared-fallback" {
				stale.share, stale.groupName = true, "g"
			}
			server, peer := net.Pipe()
			defer server.Close()
			defer peer.Close()
			current := &client{info: old.info, status: Connected, conn: server, inflight: make(map[uint16]*inflightElem), subMap: make(map[string]*subscription)}
			if scenario == "unsubscribe" {
				stale.client = current
				current.subMap[stale.topic] = stale
			}
			b.clients.Store(old.info.clientID, old)
			if scenario == "unsubscribe" {
				b.clients.Store(old.info.clientID, current)
			}
			defer b.clients.Delete(old.info.clientID)
			p := &pausedSnapshot{TopicsProvider: topics.NewMemProvider(), subs: []interface{}{stale}, ready: make(chan struct{}), release: make(chan struct{})}
			if scenario == "shared-fallback" {
				current.info.clientID = "fallback"
				active := &subscription{client: current, topic: stale.topic, qos: 0, share: true, groupName: "g", generation: 2}
				current.subMap["$share/g/"+stale.topic] = active
				b.clients.Store(current.info.clientID, current)
				defer b.clients.Delete(current.info.clientID)
				p.subs = append(p.subs, active)
			}
			name := fmt.Sprintf("snapshot-%s-%p", scenario, p)
			topics.Register(name, p)
			defer topics.Unregister(name)
			manager, err := topics.NewManager(name)
			if err != nil {
				t.Fatal(err)
			}
			b.topicsMgr = manager
			packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			packet.TopicName, packet.Qos = "snapshot/topic", 2
			result := make(chan error, 1)
			go func() { result <- b.publishLocal(packet) }()
			<-p.ready
			if scenario == "unsubscribe" {
				current.subMapMu.Lock()
				delete(current.subMap, stale.topic)
				current.subMapMu.Unlock()
			}
			if scenario == "qos" {
				current.subMap[stale.topic] = &subscription{client: current, topic: stale.topic, qos: 0, generation: stale.generation}
			}
			if scenario == "resubscribe" {
				current.subMap[stale.topic] = &subscription{client: current, topic: stale.topic, qos: 0, generation: 2}
			}
			if scenario == "shared" {
				// Same filter in a different group must not resurrect the old candidate.
				current.subMap["$share/other/"+stale.topic] = &subscription{client: current, topic: stale.topic, qos: 1, share: true, groupName: "other"}
			}
			if scenario != "shared-fallback" {
				b.clients.Store(old.info.clientID, current)
			} else {
				b.clients.Delete(old.info.clientID)
			}
			close(p.release)
			if scenario == "qos" || scenario == "shared-fallback" {
				_ = peer.SetReadDeadline(time.Now().Add(time.Second))
				got, err := packets.ReadPacket(peer)
				if err != nil {
					t.Fatal(err)
				}
				if got.(*packets.PublishPacket).Qos != 0 {
					t.Fatalf("unexpected qos: %#v", got)
				}
			}
			select {
			case err := <-result:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("publication blocked")
			}
			if scenario != "qos" && scenario != "shared-fallback" {
				_ = peer.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
				if got, err := packets.ReadPacket(peer); err == nil {
					t.Fatalf("unexpected delivery: %#v", got)
				}
			}
		})
	}
}
