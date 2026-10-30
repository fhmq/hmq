// Package inbox provides a local durable inbox for reliable.HandleInbox.
package inbox

import (
	"encoding/base64"
	"errors"
	"strings"
	"unicode/utf8"

	"github.com/fhmq/hmq/v3/broker/eventstore"
	"github.com/fhmq/hmq/v3/broker/reliable"
)

type Store struct{ store *eventstore.Store }

// BindIdentity verifies the ownership of this dedicated inbox log.
func (s *Store) BindIdentity(identity []byte) error { return s.store.BindIdentity(identity) }

func (s *Store) Identity() ([]byte, error) {
	e, err := s.store.Get("!hmq-log-identity-v1")
	return e.Payload, err
}

type Message struct {
	Peer    string
	ID      string
	Payload []byte
}

// Open opens a dedicated inbox log. Its parent directory must exist. Incomplete
// or corrupt log records cause Open to fail; they are not automatically repaired.
// Limits include completed identities, which remain reserved for deduplication.
func Open(path string, limits eventstore.Limits) (*Store, error) {
	s, err := eventstore.Open(path, limits)
	if err != nil {
		return nil, err
	}
	return &Store{store: s}, nil
}

func key(peer, id string) (string, error) {
	if peer == "" || id == "" || !utf8.ValidString(peer) || !utf8.ValidString(id) {
		return "", eventstore.ErrInvalid
	}
	return base64.RawURLEncoding.EncodeToString([]byte(peer)) + "." + base64.RawURLEncoding.EncodeToString([]byte(id)), nil
}

func (s *Store) Accept(peer, id string, payload []byte) error {
	k, err := key(peer, id)
	if err != nil {
		return err
	}
	err = s.store.Put(k, payload, []string{peer})
	if errors.Is(err, eventstore.ErrConflict) {
		return reliable.ErrConflict
	}
	return err
}

// Pending returns a snapshot in acceptance order; IDs need not be sequential.
func (s *Store) Pending(peer string, limit int) ([]Message, error) {
	events, err := s.store.Pending(peer, limit)
	if err != nil {
		return nil, err
	}
	result := make([]Message, 0, len(events))
	for _, e := range events {
		parts := strings.Split(e.ID, ".")
		if len(parts) != 2 {
			return nil, eventstore.ErrInvalid
		}
		p, err := base64.RawURLEncoding.DecodeString(parts[0])
		if err != nil || string(p) != peer {
			return nil, eventstore.ErrInvalid
		}
		id, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil || len(id) == 0 || !utf8.Valid(id) {
			return nil, eventstore.ErrInvalid
		}
		result = append(result, Message{Peer: peer, ID: string(id), Payload: e.Payload})
	}
	return result, nil
}

// Complete records application completion but retains the identity for deduplication.
// It does not make external side effects transactional.
func (s *Store) Complete(peer, id string) error {
	k, err := key(peer, id)
	if err != nil {
		return err
	}
	return s.store.Ack(k, peer)
}

func (s *Store) Close() error { return s.store.Close() }

var _ reliable.Inbox = (*Store)(nil)
