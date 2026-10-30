package replication

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"sync"
	"unicode/utf8"

	"github.com/hashicorp/raft"
)

var ErrConflict = errors.New("replication: publication ID conflict")
var ErrInvalid = errors.New("replication: invalid publication")
var ErrCapacity = errors.New("replication: partition capacity exceeded")

const MaxPayloadSize = 1 << 20

type Entry struct {
	Index   uint64 `json:"index"`
	ID      string `json:"id"`
	Payload []byte `json:"payload"`
}

type command struct {
	ID      string `json:"id"`
	Payload []byte `json:"payload"`
}
type applyResult struct {
	Index uint64
	Err   error
}
type partition struct {
	mu      sync.RWMutex
	entries []Entry
	ids     map[string]int
	bytes   int
}

func validCommand(c command) bool {
	return c.ID != "" && len(c.ID) <= 256 && utf8.ValidString(c.ID) && len(c.Payload) <= MaxPayloadSize
}

// Capacity is deterministic across every replica. Retention is deliberately
// absent until consumers have a replicated safe deletion watermark.
const maxPartitionEntries = 100000
const maxPartitionBytes = 64 << 20

func (p *partition) checkLocked(c command) (uint64, bool, error) {
	if at, exists := p.ids[c.ID]; exists {
		e := p.entries[at]
		if !bytes.Equal(e.Payload, c.Payload) {
			return 0, true, ErrConflict
		}
		return e.Index, true, nil
	}
	if len(p.entries) >= maxPartitionEntries || len(c.ID)+len(c.Payload) > maxPartitionBytes-p.bytes {
		return 0, false, ErrCapacity
	}
	return 0, false, nil
}

// Only rejects already-known failures; success must still go through Raft.
// Apply repeats this check because concurrent proposals may fill the partition.
func (p *partition) preflight(c command) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, _, err := p.checkLocked(c)
	return err
}

func (p *partition) Apply(log *raft.Log) interface{} {
	var c command
	if err := json.Unmarshal(log.Data, &c); err != nil || !validCommand(c) {
		return applyResult{Err: ErrInvalid}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	index, exists, err := p.checkLocked(c)
	if err != nil {
		return applyResult{Err: err}
	}
	if exists {
		return applyResult{Index: index}
	}
	if p.ids == nil {
		p.ids = make(map[string]int)
	}
	p.ids[c.ID] = len(p.entries)
	p.entries = append(p.entries, Entry{Index: log.Index, ID: c.ID, Payload: bytes.Clone(c.Payload)})
	p.bytes += len(c.ID) + len(c.Payload)
	return applyResult{Index: log.Index}
}

func (p *partition) entriesAfter(after uint64, limit int) []Entry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var result []Entry
	start := sort.Search(len(p.entries), func(i int) bool { return p.entries[i].Index > after })
	for _, e := range p.entries[start:] {
		e.Payload = bytes.Clone(e.Payload)
		result = append(result, e)
		if len(result) == limit {
			break
		}
	}
	return result
}

type snapshot struct {
	Version int     `json:"version"`
	Entries []Entry `json:"entries"`
}
type partitionSnapshot struct{ data snapshot }

func (p *partition) Snapshot() (raft.FSMSnapshot, error) {
	return &partitionSnapshot{snapshot{Version: 1, Entries: p.entriesAfter(0, maxPartitionEntries)}}, nil
}
func (s *partitionSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := json.NewEncoder(sink).Encode(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}
func (*partitionSnapshot) Release() {}

func (p *partition) Restore(reader io.ReadCloser) error {
	defer reader.Close()
	// JSON/base64 expands payloads; bound decoding independently of disk size.
	limited := &io.LimitedReader{R: reader, N: 2*maxPartitionBytes + 1}
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	var s snapshot
	if err := decoder.Decode(&s); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF || limited.N == 0 || s.Version != 1 || len(s.Entries) > maxPartitionEntries {
		return ErrInvalid
	}
	ids := make(map[string]int, len(s.Entries))
	var size int
	var previous uint64
	for i, e := range s.Entries {
		if !validCommand(command{e.ID, e.Payload}) || e.Index <= previous {
			return ErrInvalid
		}
		if _, exists := ids[e.ID]; exists {
			return ErrInvalid
		}
		size += len(e.ID) + len(e.Payload)
		if size > maxPartitionBytes {
			return ErrCapacity
		}
		ids[e.ID], previous = i, e.Index
	}
	p.mu.Lock()
	p.entries, p.ids, p.bytes = s.Entries, ids, size
	p.mu.Unlock()
	return nil
}
