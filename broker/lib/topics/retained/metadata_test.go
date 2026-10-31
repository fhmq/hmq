package retained

import (
	"path/filepath"
	"testing"
	"time"

	paho "github.com/eclipse/paho.golang/packets"
	"github.com/fhmq/hmq/v4/broker/packets"
)

func TestMetadataCopiesAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retained.log")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	expires := time.Now().Add(time.Hour).UTC()
	m := Message{Topic: "meta/a", Payload: []byte("value"), Metadata: &packets.PublishMetadata{
		Origin: "publisher", ExpiresAt: expires, Properties: &paho.Properties{CorrelationData: []byte{1}, User: []paho.User{{Key: "k", Value: "v"}}},
	}}
	if err := s.Put(m); err != nil {
		t.Fatal(err)
	}
	m.Metadata.Origin = "writer"
	m.Metadata.Properties.CorrelationData[0] = 2
	m.Metadata.Properties.User[0].Value = "writer"
	for restart := 0; restart < 2; restart++ {
		for _, all := range []bool{false, true, false} {
			got, ok := s.Get(m.Topic)
			if !ok {
				t.Fatal("missing message")
			}
			if all {
				got = s.All()[0]
			}
			meta := got.Metadata
			if meta == nil || meta.Origin != "publisher" || !meta.ExpiresAt.Equal(expires) || meta.Properties.CorrelationData[0] != 1 || meta.Properties.User[0].Value != "v" {
				t.Fatalf("lost or aliased metadata: %#v", meta)
			}
			meta.Origin = "reader"
			meta.Properties.CorrelationData[0] = 3
			meta.Properties.User[0].Value = "reader"
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if restart == 0 {
			s, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}
