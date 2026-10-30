package topics

import (
	"reflect"
	"sync"
	"testing"

	"github.com/eclipse/paho.mqtt.golang/packets"
)

func TestRetainedIsolationAndParent(t *testing.T) {
	p := NewMemProvider()
	msg := &packets.PublishPacket{TopicName: "a", Payload: []byte("original")}
	if err := p.Retain(msg); err != nil {
		t.Fatal(err)
	}
	msg.Payload[0] = 'X'
	if err := p.Retain(&packets.PublishPacket{TopicName: "a/b", Payload: []byte("child")}); err != nil {
		t.Fatal(err)
	}
	if err := p.Retain(&packets.PublishPacket{TopicName: "a/b"}); err != nil {
		t.Fatal(err)
	}
	var found []*packets.PublishPacket
	if err := p.Retained([]byte("a"), &found); err != nil || len(found) != 1 || string(found[0].Payload) != "original" {
		t.Fatalf("retained: %v %v", found, err)
	}
	found[0].Payload[0] = 'Y'
	found = nil
	if err := p.Retained([]byte("a"), &found); err != nil || len(found) != 1 || string(found[0].Payload) != "original" {
		t.Fatalf("retained after mutation: %v %v", found, err)
	}
}

func TestConcurrentOperations(t *testing.T) {
	p := NewMemProvider()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				p.Subscribe([]byte("a"), 1, "sub")
				var subs []interface{}
				var qos []byte
				p.Subscribers([]byte("a"), 2, &subs, &qos)
				p.Retain(&packets.PublishPacket{TopicName: "a", Payload: []byte("x")})
				var msgs []*packets.PublishPacket
				p.Retained([]byte("#"), &msgs)
				p.Close()
				NewManager("mem")
			}
		}()
	}
	wg.Wait()
}

func TestGrantedQoS(t *testing.T) {
	p := NewMemProvider()
	p.Subscribe([]byte("a"), 0, "sub")
	var subs []interface{}
	var qos []byte
	if err := p.Subscribers([]byte("a"), 2, &subs, &qos); err != nil || len(qos) != 1 || qos[0] != 0 {
		t.Fatalf("qos %v: %v", qos, err)
	}
}

func TestProtocolTopicMatching(t *testing.T) {
	p := NewMemProvider()
	for _, filter := range []string{"#", "+/b", "a/+", "a/#", "/b", "$SYS/#"} {
		if _, err := p.Subscribe([]byte(filter), 1, filter); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		topic string
		want  map[string]bool
	}{
		{"/b", map[string]bool{"#": true, "+/b": true, "/b": true}},
		{"a/", map[string]bool{"#": true, "a/+": true, "a/#": true}},
		{"a", map[string]bool{"#": true, "a/#": true}},
		{"$SYS/b", map[string]bool{"$SYS/#": true}},
	} {
		var subs []interface{}
		var qos []byte
		if err := p.Subscribers([]byte(tc.topic), 1, &subs, &qos); err != nil {
			t.Fatal(err)
		}
		got := map[string]bool{}
		for _, sub := range subs {
			got[sub.(string)] = true
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%q: got %v want %v", tc.topic, got, tc.want)
		}
	}
}

func TestProtocolRetainedAndValidation(t *testing.T) {
	p := NewMemProvider()
	for _, topic := range []string{"a", "a/", "/b", "$SYS/b"} {
		if err := p.Retain(&packets.PublishPacket{TopicName: topic, Payload: []byte("x")}); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		filter string
		want   int
	}{{"#", 3}, {"+", 1}, {"a/+", 1}, {"a/#", 2}, {"$SYS/#", 1}, {"/b", 1}} {
		var msgs []*packets.PublishPacket
		if err := p.Retained([]byte(tc.filter), &msgs); err != nil || len(msgs) != tc.want {
			t.Errorf("%q: got %d, error %v", tc.filter, len(msgs), err)
		}
	}
	if err := p.Retain(&packets.PublishPacket{TopicName: "missing"}); err != nil {
		t.Fatal(err)
	}
	for _, filter := range []string{"", "a/#/b", "a+", "##", "a/\x00"} {
		if _, err := p.Subscribe([]byte(filter), 0, "x"); err == nil {
			t.Errorf("accepted filter %q", filter)
		}
		var msgs []*packets.PublishPacket
		if err := p.Retained([]byte(filter), &msgs); err == nil {
			t.Errorf("accepted retained filter %q", filter)
		}
	}
	for _, topic := range []string{"", "a/+", "a/#", "a/\x00"} {
		if err := p.Retain(&packets.PublishPacket{TopicName: topic, Payload: []byte("x")}); err == nil {
			t.Errorf("accepted topic %q", topic)
		}
	}
}

func TestManagersHaveIndependentMemoryProviders(t *testing.T) {
	a, err := NewManager("mem")
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewManager("mem")
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for _, tc := range []struct {
		m     *Manager
		value string
	}{{a, "a"}, {b, "b"}} {
		if _, err := tc.m.Subscribe([]byte("same"), 1, tc.value); err != nil {
			t.Fatal(err)
		}
		if err := tc.m.Retain(&packets.PublishPacket{TopicName: "same", Payload: []byte(tc.value)}); err != nil {
			t.Fatal(err)
		}
	}
	check := func(m *Manager, want string) {
		t.Helper()
		var subs []interface{}
		var qos []byte
		if err := m.Subscribers([]byte("same"), 1, &subs, &qos); err != nil || len(subs) != 1 || subs[0] != want {
			t.Fatalf("subscribers = %v, error = %v; want %s", subs, err, want)
		}
		var msgs []*packets.PublishPacket
		if err := m.Retained([]byte("same"), &msgs); err != nil || len(msgs) != 1 || string(msgs[0].Payload) != want {
			t.Fatalf("retained = %v, error = %v; want %s", msgs, err, want)
		}
	}
	check(a, "a")
	check(b, "b")
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	check(b, "b")
}
