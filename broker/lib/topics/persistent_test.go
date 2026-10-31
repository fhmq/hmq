package topics

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/fhmq/hmq/v4/broker/packets"
	"github.com/fhmq/hmq/v4/broker/storage"
)

func TestPersistentFailureStopsRetainedAccess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retained.log")
	m, err := NewPersistentManager(path)
	if errors.Is(err, storage.ErrUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	msg := persistentMessage("a", "")
	msg.Payload = bytes.Repeat([]byte("x"), storage.MaxRecordSize)
	if err := m.Retain(msg); !errors.Is(err, storage.ErrTooLarge) {
		t.Fatal(err)
	}
	var found []*packets.PublishPacket
	if err := m.Retained([]byte("#"), &found); !errors.Is(err, storage.ErrTooLarge) {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err = NewPersistentManager(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if err := m.Retained([]byte("#"), &found); err != nil || len(found) != 0 {
		t.Fatalf("%v %v", found, err)
	}
}

func persistentMessage(topic, payload string) *packets.PublishPacket {
	m := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	m.TopicName, m.Qos, m.Payload = topic, 1, []byte(payload)
	return m
}

func TestPersistentManagerRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retained.log")
	m, err := NewPersistentManager(path)
	if errors.Is(err, storage.ErrUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range []*packets.PublishPacket{persistentMessage("a/1", "one"), persistentMessage("a/2", "two"), persistentMessage("a/2", "")} {
		if err := m.Retain(msg); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err = NewPersistentManager(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	var messages []*packets.PublishPacket
	if err := m.Retained([]byte("a/+"), &messages); err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].TopicName != "a/1" || string(messages[0].Payload) != "one" || messages[0].Qos != 1 {
		t.Fatalf("unexpected: %#v", messages)
	}
}

func TestPersistentManagerConcurrent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "retained.log")
	m, err := NewPersistentManager(path)
	if errors.Is(err, storage.ErrUnsupported) {
		t.Skip(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				if err := m.Retain(persistentMessage(fmt.Sprintf("a/%d", i), fmt.Sprint(j))); err != nil {
					t.Error(err)
				}
				var found []*packets.PublishPacket
				if err := m.Retained([]byte("a/#"), &found); err != nil {
					t.Error(err)
				}
			}
		}(i)
	}
	wg.Wait()
	var before []*packets.PublishPacket
	if err := m.Retained([]byte("a/#"), &before); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	m, err = NewPersistentManager(path)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	var after []*packets.PublishPacket
	if err := m.Retained([]byte("a/#"), &after); err != nil {
		t.Fatal(err)
	}
	values := map[string]string{}
	for _, msg := range before {
		values[msg.TopicName] = string(msg.Payload)
	}
	if len(after) != 12 {
		t.Fatal(len(after))
	}
	for _, msg := range after {
		if values[msg.TopicName] != string(msg.Payload) {
			t.Fatal(msg.TopicName)
		}
	}
}
