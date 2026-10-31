package sessions

import (
	"fmt"
	"sync"
	"testing"

	"github.com/fhmq/hmq/v4/broker/packets"
)

func TestSessionLifecycle(t *testing.T) {
	s := &Session{id: "stored"}
	if s.ID() != "stored" || s.WillFlag() || s.CleanSession() {
		t.Fatal("unexpected uninitialized state")
	}
	s.SetWillFlag(true)
	if s.Init(nil) == nil || s.Update(nil) == nil {
		t.Fatal("nil connect accepted")
	}
	msg := &packets.ConnectPacket{ClientIdentifier: "client"}
	if err := s.Init(msg); err != nil {
		t.Fatal(err)
	}
	if s.Init(msg) == nil {
		t.Fatal("duplicate init accepted")
	}
	if err := s.AddTopic("a", 1); err != nil {
		t.Fatal(err)
	}
	topics, qos, err := s.Topics()
	if err != nil || len(topics) != 1 || topics[0] != "a" || qos[0] != 1 {
		t.Fatal(topics, qos, err)
	}
	if err := s.RemoveTopic("a"); err != nil {
		t.Fatal(err)
	}
}

func TestSubscriptionGenerationLifecycle(t *testing.T) {
	s := &Session{}
	if err := s.Init(&packets.ConnectPacket{ClientIdentifier: "client"}); err != nil {
		t.Fatal(err)
	}
	first := NewSubscriptionGeneration()
	if err := s.AddTopicGeneration("a", 1, first); err != nil {
		t.Fatal(err)
	}
	if got := s.TopicGeneration("a"); got != first {
		t.Fatalf("generation = %d", got)
	}
	if err := s.AddTopicGeneration("a", 0, first); err != nil {
		t.Fatal(err)
	}
	if got := s.TopicGeneration("a"); got != first {
		t.Fatalf("updated generation = %d", got)
	}
	if err := s.RemoveTopic("a"); err != nil {
		t.Fatal(err)
	}
	if got := s.TopicGeneration("a"); got != 0 {
		t.Fatalf("removed generation = %d", got)
	}
	if err := s.AddTopic("a", 1); err != nil {
		t.Fatal(err)
	}
	if got := s.TopicGeneration("a"); got == 0 || got == first {
		t.Fatalf("new generation = %d", got)
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := &Session{}
	if err := s.Init(&packets.ConnectPacket{ClientIdentifier: "client"}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("test-concurrent-%d", i)
			for j := 0; j < 100; j++ {
				Register(name, NewMemProvider())
				_, _ = NewManager(name)
				Unregister(name)
				_ = s.Update(&packets.ConnectPacket{ClientIdentifier: name})
				_ = s.ID()
				s.SetWillFlag(true)
				_ = s.WillFlag()
				_ = s.AddTopic(name, 1)
				_, _, _ = s.Topics()
				_ = s.RetainMessage(&packets.PublishPacket{Payload: []byte(name)})
				_ = s.Retained()
				_ = s.Will()
			}
		}(i)
	}
	wg.Wait()
}

func TestSessionOwnsInputPackets(t *testing.T) {
	s := &Session{}
	msg := &packets.ConnectPacket{ClientIdentifier: "original", WillFlag: true, WillMessage: []byte("will"), Password: []byte("secret")}
	if err := s.Init(msg); err != nil {
		t.Fatal(err)
	}
	msg.ClientIdentifier = "changed"
	msg.WillMessage[0] = 'X'
	msg.Password[0] = 'X'
	if s.ID() != "original" || string(s.Will().Payload) != "will" || string(s.cmsg.Password) != "secret" {
		t.Fatal("connect packet aliases caller")
	}
	s.Will().Payload[0] = 'Y'
	if string(s.Will().Payload) != "will" || string(s.cmsg.WillMessage) != "will" {
		t.Fatal("will aliases connect packet")
	}
	update := &packets.ConnectPacket{ClientIdentifier: "updated", WillMessage: []byte("next")}
	if err := s.Update(update); err != nil {
		t.Fatal(err)
	}
	update.ClientIdentifier = "changed"
	update.WillMessage[0] = 'X'
	if s.ID() != "updated" || string(s.cmsg.WillMessage) != "next" {
		t.Fatal("update aliases caller")
	}
	pub := &packets.PublishPacket{Payload: []byte("payload")}
	_ = s.RetainMessage(pub)
	pub.Payload[0] = 'X'
	if string(s.Retained().Payload) != "payload" {
		t.Fatal("retained packet aliases caller")
	}
	s.Retained().Payload[0] = 'Y'
	if string(s.Retained().Payload) != "payload" {
		t.Fatal("retained snapshot aliases session")
	}
	_ = s.RetainMessage(nil)
	if s.Retained() != nil {
		t.Fatal("retained packet not cleared")
	}
}

func TestMemManagersAreIndependent(t *testing.T) {
	a, err := NewManager("mem")
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewManager("mem")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.New("same"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get("same"); err == nil {
		t.Fatal("manager shares sessions")
	}
	if _, err := b.New("same"); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if b.Count() != 1 {
		t.Fatal("closing one manager changed another")
	}
	if _, err := b.Get("same"); err != nil {
		t.Fatal(err)
	}
	b.Del("same")
	if b.Count() != 0 {
		t.Fatal("delete failed")
	}
}
