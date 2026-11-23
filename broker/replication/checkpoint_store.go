package replication

import (
	"encoding/binary"

	"github.com/hashicorp/raft"
)

const snapshotRequirement = "hmq/required-snapshot/v1"

// Record the recovery dependency after the snapshot is installed, but before
// Raft can delete the covered log. Do not fall back behind that dependency.
type checkpointSnapshots struct {
	raft.SnapshotStore
	wal *Store
}

func (s checkpointSnapshots) Create(version raft.SnapshotVersion, index, term uint64, conf raft.Configuration, confIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {
	sink, err := s.SnapshotStore.Create(version, index, term, conf, confIndex, trans)
	if err != nil {
		return nil, err
	}
	return checkpointSink{sink, s.wal, index}, nil
}

func (s checkpointSnapshots) List() ([]*raft.SnapshotMeta, error) {
	required, err := s.wal.GetUint64([]byte(snapshotRequirement))
	if err != nil {
		return nil, err
	}
	all, err := s.SnapshotStore.List()
	if err != nil || required == 0 {
		return all, err
	}
	var usable []*raft.SnapshotMeta
	for _, meta := range all {
		if meta.Index >= required {
			usable = append(usable, meta)
		}
	}
	if len(usable) == 0 {
		return nil, ErrStoreInvalid
	}
	return usable, nil
}

type checkpointSink struct {
	raft.SnapshotSink
	wal   *Store
	index uint64
}

func (s checkpointSink) Close() error {
	if err := s.SnapshotSink.Close(); err != nil {
		return err
	}
	s.wal.mu.Lock()
	defer s.wal.mu.Unlock()
	if s.wal.unavailable {
		return ErrUnavailable
	}
	if prior := s.wal.stable[snapshotRequirement]; len(prior) != 0 {
		if len(prior) != 8 {
			return ErrStoreInvalid
		}
		if binary.BigEndian.Uint64(prior) >= s.index {
			return nil
		}
	}
	var index [8]byte
	binary.BigEndian.PutUint64(index[:], s.index)
	return s.wal.append(storeRecord{Version: 1, Op: "set", Key: []byte(snapshotRequirement), Value: index[:]})
}
