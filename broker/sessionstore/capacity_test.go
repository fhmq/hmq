package sessionstore

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	mqtt5 "github.com/eclipse/paho.golang/packets"
	"github.com/fhmq/hmq/v4/broker/packets"
)

func TestByteQuotaSurvivesReplayAndDrain(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions.wal")
	msg := Message{Topic: "filter/one", QoS: 1, Payload: []byte("body"), QueuedAt: time.Now().UTC(),
		Metadata: &packets.PublishMetadata{Properties: &mqtt5.Properties{ContentType: "text/plain"}}}
	limit := Limits{MaxPendingBytes: messageBytes(msg)}
	open := func(l Limits) *Store {
		t.Helper()
		s, err := OpenLimited(path, l)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	s := open(limit)
	if _, err := s.Put("sub", 0, &State{Subscriptions: map[string]byte{"filter/#": 2}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Fanout(msg, limit); err != nil {
		t.Fatal(err)
	}
	apply := func(batch Batch) error {
		t.Helper()
		_, version, err := s.Get("sub")
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.Apply("sub", version, batch)
		return err
	}
	small := Message{Topic: "filter/two", QoS: 1}
	if err := apply(Batch{Move: []uint16{9}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Fanout(small, limit); !errors.Is(err, ErrQuota) {
		t.Fatalf("inflight should still count: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	limit.MaxPendingBytes--
	s = open(limit)
	if got := s.Capacity(); got.Queued.Messages != 0 || got.Outbound.Bytes != limit.MaxPendingBytes+1 {
		t.Fatalf("historical accepted data was not recovered: %+v", got)
	}
	if err := s.Fanout(small, limit); !errors.Is(err, ErrQuota) {
		t.Fatalf("historical bytes must count against the new limit: %v", err)
	}
	if err := apply(Batch{Acks: []Ack{{9, "PUBACK"}}}); err != nil {
		t.Fatalf("an over-limit session must still be able to drain: %v", err)
	}
	if err := s.Fanout(small, limit); err != nil {
		t.Fatalf("quota must be available after ACK: %v", err)
	}
}
