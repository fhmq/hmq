// Package retained provides a standalone durable retained-message store.
package retained

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/fhmq/hmq/v3/broker/storage"
)

var ErrInvalid = errors.New("retained: invalid record")

type Message struct {
	Topic   string `json:"topic"`
	QoS     byte   `json:"qos"`
	Payload []byte `json:"payload"`
}

type record struct {
	Version int     `json:"version"`
	Delete  bool    `json:"delete"`
	Message Message `json:"message"`
}

type Store struct {
	mu       sync.Mutex
	log      *storage.Log
	messages map[string]Message
}

func valid(m Message) bool {
	return m.Topic != "" && len(m.Topic) <= 65535 && utf8.ValidString(m.Topic) && !strings.ContainsAny(m.Topic, "\x00+#") && m.QoS <= 2
}

// Open recovers all records, rejecting invalid application records.
func Open(path string) (*Store, error) {
	l, err := storage.Open(path)
	if err != nil {
		return nil, err
	}
	s := &Store{log: l, messages: make(map[string]Message)}
	err = l.Replay(func(data []byte) error {
		var r record
		if e := json.Unmarshal(data, &r); e != nil || !utf8.Valid(data) || r.Version != 1 || !valid(r.Message) || (r.Delete && (r.Message.QoS != 0 || len(r.Message.Payload) != 0)) || (!r.Delete && len(r.Message.Payload) == 0) {
			return fmt.Errorf("%w: malformed log entry", ErrInvalid)
		}
		if r.Delete {
			delete(s.messages, r.Message.Topic)
		} else {
			s.messages[r.Message.Topic] = r.Message
		}
		return nil
	})
	if err != nil {
		l.Close()
		return nil, err
	}
	return s, nil
}

// Put persists a message before publishing it to readers. Payload is copied.
func (s *Store) Put(m Message) error {
	if !valid(m) || len(m.Payload) == 0 {
		return ErrInvalid
	}
	m.Payload = append([]byte(nil), m.Payload...)
	return s.apply(record{Version: 1, Message: m})
}

// Delete persists a deletion, including when the topic is absent.
func (s *Store) Delete(topic string) error {
	m := Message{Topic: topic}
	if !valid(m) {
		return ErrInvalid
	}
	return s.apply(record{Version: 1, Delete: true, Message: m})
}

func (s *Store) apply(r record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if err = s.log.Append(data); err != nil {
		return err
	}
	if r.Delete {
		delete(s.messages, r.Message.Topic)
	} else {
		s.messages[r.Message.Topic] = r.Message
	}
	return nil
}

// Get returns an independent copy of the message.
func (s *Store) Get(topic string) (Message, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.messages[topic]
	m.Payload = append([]byte(nil), m.Payload...)
	return m, ok
}

// All returns independent copies of the current messages in unspecified order.
func (s *Store) All() []Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]Message, 0, len(s.messages))
	for _, m := range s.messages {
		m.Payload = append([]byte(nil), m.Payload...)
		result = append(result, m)
	}
	return result
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.Close()
}
