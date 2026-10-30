// Package replication provides durable storage for Hashicorp Raft.
package replication

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/fhmq/hmq/v3/broker/storage"
	"github.com/hashicorp/raft"
)

var ErrUnavailable = errors.New("replication: store closed or poisoned; reopen required")
var ErrStoreInvalid = errors.New("replication: invalid store operation")

type storeWAL interface {
	Append([]byte) error
	Replay(func([]byte) error) error
	Close() error
}

type storeRecord struct {
	Version int
	Op      string
	Logs    []raft.Log `json:",omitempty"`
	Key     []byte     `json:",omitempty"`
	Value   []byte     `json:",omitempty"`
	Min     uint64     `json:",omitempty"`
	Max     uint64     `json:",omitempty"`
}

// Store implements Raft's LogStore and StableStore on a single WAL file.
// The parent directory must exist. A failed append makes all operations except
// Close unavailable, since the durable outcome may be uncertain. Reopening a
// torn WAL fails closed using storage's corruption checks; it never repairs it.
// WAL history is append-only; DeleteRange removes logical entries, not bytes.
type Store struct {
	mu          sync.RWMutex
	wal         storeWAL
	logs        map[uint64]raft.Log
	stable      map[string][]byte
	first, last uint64
	unavailable bool
}

var _ raft.LogStore = (*Store)(nil)
var _ raft.StableStore = (*Store)(nil)

func OpenStore(path string) (*Store, error) {
	w, err := storage.Open(path)
	if err != nil {
		return nil, err
	}
	s := &Store{wal: w, logs: make(map[uint64]raft.Log), stable: make(map[string][]byte)}
	err = w.Replay(func(b []byte) error {
		var r storeRecord
		d := json.NewDecoder(bytes.NewReader(b))
		d.DisallowUnknownFields()
		if err := d.Decode(&r); err != nil {
			return err
		}
		if err := d.Decode(new(any)); err != io.EOF {
			return ErrStoreInvalid
		}
		if err := r.validate(); err != nil {
			return err
		}
		s.apply(r)
		return nil
	})
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("replication: replay: %w", err)
	}
	return s, nil
}

func (r storeRecord) validate() error {
	if r.Version != 1 {
		return ErrStoreInvalid
	}
	switch r.Op {
	case "logs":
		if len(r.Logs) == 0 || len(r.Key) != 0 || len(r.Value) != 0 || r.Min != 0 || r.Max != 0 {
			return ErrStoreInvalid
		}
		for _, l := range r.Logs {
			if l.Index == 0 {
				return ErrStoreInvalid
			}
		}
	case "set":
		if len(r.Logs) != 0 || r.Min != 0 || r.Max != 0 {
			return ErrStoreInvalid
		}
	case "delete":
		if len(r.Logs) != 0 || len(r.Key) != 0 || len(r.Value) != 0 || r.Min > r.Max {
			return ErrStoreInvalid
		}
	default:
		return ErrStoreInvalid
	}
	return nil
}

func copyLog(l raft.Log) raft.Log {
	l.Data = bytes.Clone(l.Data)
	l.Extensions = bytes.Clone(l.Extensions)
	return l
}

func (s *Store) apply(r storeRecord) {
	switch r.Op {
	case "logs":
		for _, l := range r.Logs {
			s.logs[l.Index] = l
			if s.first == 0 || l.Index < s.first {
				s.first = l.Index
			}
			if l.Index > s.last {
				s.last = l.Index
			}
		}
	case "set":
		s.stable[string(r.Key)] = r.Value
	case "delete":
		// Iterate stored keys, not the numeric range (which may span MaxUint64).
		for i := range s.logs {
			if i >= r.Min && i <= r.Max {
				delete(s.logs, i)
			}
		}
		s.first, s.last = 0, 0
		for i := range s.logs {
			if s.first == 0 || i < s.first {
				s.first = i
			}
			if i > s.last {
				s.last = i
			}
		}
	}
}

// append is called with mu held; memory is published only after durable success.
func (s *Store) append(r storeRecord) error {
	if s.unavailable {
		return ErrUnavailable
	}
	if err := r.validate(); err != nil {
		return err
	}
	b, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if len(b) > storage.MaxRecordSize {
		return storage.ErrTooLarge
	}
	if err := s.wal.Append(b); err != nil {
		s.unavailable = true
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	s.apply(r)
	return nil
}

func (s *Store) FirstIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.unavailable {
		return 0, ErrUnavailable
	}
	return s.first, nil
}

func (s *Store) LastIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.unavailable {
		return 0, ErrUnavailable
	}
	return s.last, nil
}

func (s *Store) GetLog(index uint64, out *raft.Log) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.unavailable {
		return ErrUnavailable
	}
	if out == nil {
		return ErrStoreInvalid
	}
	l, ok := s.logs[index]
	if !ok {
		return raft.ErrLogNotFound
	}
	*out = copyLog(l)
	return nil
}

func (s *Store) StoreLog(l *raft.Log) error { return s.StoreLogs([]*raft.Log{l}) }

// StoreLogs commits the whole batch as one WAL record. Oversized batches are
// rejected before writing, rather than split into partially durable records.
// As with other Go APIs, callers must not mutate inputs during the call.
func (s *Store) StoreLogs(logs []*raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unavailable {
		return ErrUnavailable
	}
	if len(logs) == 0 {
		return nil
	}
	r := storeRecord{Version: 1, Op: "logs", Logs: make([]raft.Log, len(logs))}
	for i, l := range logs {
		if l == nil {
			return ErrStoreInvalid
		}
		r.Logs[i] = copyLog(*l)
	}
	return s.append(r)
}

func (s *Store) DeleteRange(min, max uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.append(storeRecord{Version: 1, Op: "delete", Min: min, Max: max})
}

func (s *Store) Set(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.append(storeRecord{Version: 1, Op: "set", Key: bytes.Clone(key), Value: bytes.Clone(value)})
}

func (s *Store) Get(key []byte) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.unavailable {
		return nil, ErrUnavailable
	}
	return bytes.Clone(s.stable[string(key)]), nil
}

func (s *Store) SetUint64(key []byte, value uint64) error {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], value)
	return s.Set(key, b[:])
}

func (s *Store) GetUint64(key []byte) (uint64, error) {
	b, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if b == nil {
		return 0, nil
	}
	if len(b) != 8 {
		return 0, ErrStoreInvalid
	}
	return binary.BigEndian.Uint64(b), nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal == nil {
		return ErrUnavailable
	}
	s.unavailable = true
	err := s.wal.Close()
	s.wal = nil
	return err
}
