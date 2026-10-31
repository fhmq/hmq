package topics

import (
	"path/filepath"
	"testing"
	"time"

	paho "github.com/eclipse/paho.golang/packets"
	"github.com/fhmq/hmq/v4/broker/packets"
)

func TestRetainedMetadataIsolation(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(map[bool]string{false: "memory", true: "persistent"}[persistent], func(t *testing.T) {
			var provider TopicsProvider = NewMemProvider()
			if persistent {
				m, err := NewPersistentManager(filepath.Join(t.TempDir(), "retained.log"))
				if err != nil {
					t.Fatal(err)
				}
				provider = m.p
			}
			defer provider.Close()
			expiry := uint32(60)
			msg := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			msg.TopicName, msg.Payload = "meta/a", []byte("value")
			msg.Metadata = &packets.PublishMetadata{Origin: "publisher", Properties: &paho.Properties{
				MessageExpiry: &expiry, CorrelationData: []byte{1}, User: []paho.User{{Key: "key", Value: "value"}},
			}}
			if err := provider.Retain(msg); err != nil {
				t.Fatal(err)
			}
			msg.Metadata.Origin = "changed"
			*msg.Metadata.Properties.MessageExpiry = 1
			msg.Metadata.Properties.CorrelationData[0] = 2
			msg.Metadata.Properties.User[0].Value = "changed"
			for i := 0; i < 2; i++ {
				var got []*packets.PublishPacket
				if err := provider.Retained([]byte("meta/+"), &got); err != nil {
					t.Fatal(err)
				}
				if len(got) != 1 {
					t.Fatalf("got %d messages", len(got))
				}
				m := got[0].Metadata
				if m == nil || m.Origin != "publisher" || *m.Properties.MessageExpiry != 60 || m.Properties.CorrelationData[0] != 1 || m.Properties.User[0].Value != "value" {
					t.Fatalf("metadata changed: %#v", m)
				}
				m.Origin = "reader"
				*m.Properties.MessageExpiry = 2
				m.Properties.CorrelationData[0] = 3
				m.Properties.User[0].Value = "reader"
			}
		})
	}
}

func TestRetainedMetadataExpiryAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retained.log")
	m, err := NewPersistentManager(path)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(100 * time.Millisecond)
	for _, topic := range []string{"meta/expired", "meta/live", "$SYS/expired"} {
		msg := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		msg.TopicName, msg.Payload = topic, []byte("value")
		msg.Metadata = &packets.PublishMetadata{Origin: "publisher", Properties: &paho.Properties{ContentType: "text/plain"}}
		if topic != "meta/live" {
			msg.Metadata.ExpiresAt = deadline
		}
		if err := m.Retain(msg); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Until(deadline) + time.Millisecond)
	m, err = NewPersistentManager(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for _, filter := range []string{"#", "meta/+", "meta/#", "meta/expired", "$SYS/#"} {
		var got []*packets.PublishPacket
		if err := m.Retained([]byte(filter), &got); err != nil {
			t.Fatal(err)
		}
		want := 1
		if filter == "meta/expired" || filter == "$SYS/#" {
			want = 0
		}
		if len(got) != want {
			t.Fatalf("%s: got %d, want %d", filter, len(got), want)
		}
		if want == 1 && (got[0].Metadata == nil || got[0].Metadata.Origin != "publisher" || got[0].Metadata.Properties.ContentType != "text/plain") {
			t.Fatalf("lost metadata: %#v", got[0])
		}
	}
}
