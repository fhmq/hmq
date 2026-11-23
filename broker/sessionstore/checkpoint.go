package sessionstore

import (
	"bytes"
	"encoding/json"
	"io"
)

const MaxCheckpointBytes = 128 << 20

type checkpoint struct {
	Sequence uint64                    `json:"sequence"`
	Entries  map[string]checkpointItem `json:"entries"`
}

type checkpointItem struct {
	Version uint64 `json:"version"`
	State   *State `json:"state"`
}

// Checkpoint captures the replica's complete state, including version tombstones.
func (s *Store) Checkpoint() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log != nil || s.closed {
		return nil, ErrInvalid
	}
	c := checkpoint{Sequence: s.sequence, Entries: make(map[string]checkpointItem, len(s.entries))}
	for id, e := range s.entries {
		c.Entries[id] = checkpointItem{e.version, e.state}
	}
	data, err := json.Marshal(c)
	if len(data) > MaxCheckpointBytes {
		return nil, ErrInvalid
	}
	return data, err
}

// RestoreCheckpoint replaces only a materialized replica, never a standalone WAL.
func (s *Store) RestoreCheckpoint(data []byte) error {
	if len(data) == 0 || len(data) > MaxCheckpointBytes {
		return ErrCorrupt
	}
	var c checkpoint
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&c) != nil || d.Decode(new(any)) != io.EOF || c.Entries == nil {
		return ErrCorrupt
	}
	entries := make(map[string]entry, len(c.Entries))
	for id, item := range c.Entries {
		if item.Version == 0 || item.Version > c.Sequence || validate(id, item.State) != nil {
			return ErrCorrupt
		}
		entries[id] = entry{item.Version, item.State, pending(item.State)}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log != nil || s.closed {
		return ErrInvalid
	}
	s.sequence, s.entries = c.Sequence, entries
	return nil
}
