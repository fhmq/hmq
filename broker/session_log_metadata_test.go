package broker

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/fhmq/hmq/v4/broker/packets"
	"github.com/fhmq/hmq/v4/broker/sessionstore"
)

func TestSessionLogMetadataExpiryAndInflightRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.log")
	s, err := sessionstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	expired := &packets.PublishMetadata{Origin: "expired", ExpiresAt: time.Now().UTC().Add(-time.Hour)}
	live := &packets.PublishMetadata{Origin: "live", ExpiresAt: time.Now().UTC().Add(time.Hour)}
	_, err = s.Put("metadata", 0, &sessionstore.State{
		Queue:    []sessionstore.Message{{Topic: "topic", Payload: []byte("expired"), QoS: 1, Metadata: expired}, {Topic: "topic", Payload: []byte("live"), QoS: 1, Metadata: live}},
		Inflight: map[uint16]sessionstore.Inflight{7: {Message: sessionstore.Message{Topic: "topic", Payload: []byte("started"), QoS: 2, Metadata: expired}, Phase: sessionstore.Publish, Sequence: 1}}, LastSequence: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := NewBroker(&Config{ExperimentalSessionLogPath: path})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	peer, done := logMQTTConnect(t, b, "metadata", true)
	for _, want := range []string{"started", "live"} {
		packet, err := packets.ReadPacket(peer)
		if err != nil {
			t.Fatal(err)
		}
		p, ok := packet.(*packets.PublishPacket)
		if !ok || string(p.Payload) != want {
			t.Fatalf("got %#v, want %s", packet, want)
		}
	}
	value, ok := b.clients.Load("metadata")
	if !ok {
		t.Fatal("connected client missing")
	}
	c := value.(*client)
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Payload, p.Qos, p.Metadata = "topic", []byte("expired"), 1, expired
	if err := c.enqueueSessionDelivery(p); err != nil {
		t.Fatal(err)
	}
	peer.Close()
	waitLogDone(t, done)
	b.Close()
	s, err = sessionstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	state, _, err := s.Get("metadata")
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Queue) != 0 || len(state.Inflight) != 2 || state.LastSequence != 2 {
		t.Fatalf("expired queue was not removed before start: %#v", state)
	}
	for _, item := range state.Inflight {
		if item.Message.Metadata == nil {
			t.Fatal("lost metadata")
		}
		if string(item.Message.Payload) == "expired" {
			t.Fatal("expired queue entered inflight")
		}
	}
	if state.Inflight[7].Message.Metadata.Origin != "expired" || state.Inflight[7].Phase != sessionstore.Publish {
		t.Fatal("started exchange lost state")
	}
}
