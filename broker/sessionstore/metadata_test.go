package sessionstore

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	mqtt5 "github.com/eclipse/paho.golang/packets"
	"github.com/fhmq/hmq/v4/broker/packets"
	"github.com/fhmq/hmq/v4/broker/storage"
)

func TestMetadataIsolationAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sessions")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	metadata := &packets.PublishMetadata{Origin: "publisher", ExpiresAt: time.Now().UTC().Add(time.Hour), Properties: &mqtt5.Properties{CorrelationData: []byte("data"), User: []mqtt5.User{{Key: "key", Value: "value"}}}}
	want := packets.CloneMetadata(metadata)
	v, err := s.Put("client", 0, &State{Queue: []Message{{Topic: "topic", QoS: 2, Metadata: metadata}, {Topic: "topic", QoS: 1, Metadata: metadata}}})
	if err != nil {
		t.Fatal(err)
	}
	mutate := func(m *packets.PublishMetadata) {
		m.Origin = "changed"
		m.Properties.CorrelationData[0] = 'x'
		m.Properties.User[0].Value = "changed"
	}
	mutate(metadata)
	v, err = s.Apply("client", v, Batch{Move: []uint16{7}})
	if err != nil {
		t.Fatal(err)
	}
	check := func(m *packets.PublishMetadata) {
		t.Helper()
		if !reflect.DeepEqual(m, want) {
			t.Fatalf("metadata = %#v, want %#v", m, want)
		}
		mutate(m)
	}
	for restart := 0; restart < 2; restart++ {
		state, _, err := s.Get("client")
		if err != nil {
			t.Fatal(err)
		}
		check(state.Queue[0].Metadata)
		check(state.Inflight[7].Message.Metadata)
		queue, _, err := s.ReadQueue("client", 0, 1, 1024)
		if err != nil {
			t.Fatal(err)
		}
		check(queue[0].Metadata)
		inflight, _, err := s.ReadInflight("client", 0, 1, 1024)
		if err != nil {
			t.Fatal(err)
		}
		check(inflight[0].Inflight.Message.Metadata)
		_, _, item, found, err := s.Inspect("client", 7)
		if err != nil || !found {
			t.Fatalf("inspect: %v %v", found, err)
		}
		check(item.Message.Metadata)
		if restart == 0 {
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s, err = Open(path)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestMetadataReadBudgets(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, metadata := range []*packets.PublishMetadata{nil, {Origin: "publisher", Properties: &mqtt5.Properties{CorrelationData: []byte(strings.Repeat("x", 1024)), User: []mqtt5.User{{Key: "key", Value: strings.Repeat("value", 1024)}}}}} {
		name := "legacy"
		budget := 3
		if metadata != nil {
			name = "metadata"
			data, err := json.Marshal(metadata)
			if err != nil {
				t.Fatal(err)
			}
			budget += len(data)
		}
		t.Run(name, func(t *testing.T) {
			message := Message{Topic: "topic", Payload: []byte("abc"), QoS: 1, Metadata: metadata}
			_, err := s.Put(name, 0, &State{Queue: []Message{message, message}, LastSequence: 2, Inflight: map[uint16]Inflight{1: {Message: message, Phase: Publish, Sequence: 1}, 2: {Message: message, Phase: Publish, Sequence: 2}}})
			if err != nil {
				t.Fatal(err)
			}
			reads := map[string]func(int) (int, error){
				"queue":    func(bound int) (int, error) { page, _, err := s.ReadQueue(name, 0, 2, bound); return len(page), err },
				"inflight": func(bound int) (int, error) { page, _, err := s.ReadInflight(name, 0, 2, bound); return len(page), err },
			}
			for kind, read := range reads {
				for _, tc := range []struct {
					bound, count int
					tooLarge     bool
				}{{budget - 1, 0, true}, {budget, 1, false}, {2*budget - 1, 1, false}, {2 * budget, 2, false}} {
					count, err := read(tc.bound)
					if count != tc.count || (tc.tooLarge && !errors.Is(err, storage.ErrTooLarge)) || (!tc.tooLarge && err != nil) {
						t.Fatalf("%s bound %d: count=%d err=%v", kind, tc.bound, count, err)
					}
				}
			}
		})
	}
}
