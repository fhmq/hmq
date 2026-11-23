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
	ID       string    `json:"id"`
	Payload  []byte    `json:"payload"`
	Floor    uint64    `json:"floor,omitempty"`
	Progress *Progress `json:"progress,omitempty"`
}
type applyResult struct {
	Index uint64
	Err   error
}
type partition struct {
	mu            sync.RWMutex
	entries       []Entry
	ids           map[string]int
	bytes         int
	floor         uint64
	checkpoint    []byte
	progress      map[string]uint64
	members       map[string]bool
	compact       func([]byte, []Entry) ([]byte, error)
	compactFailed bool
}

type Progress struct {
	Member string
	Cursor uint64
}

var ErrCompacted = errors.New("replication: operation predates retained history; outcome uncertain")

const minCompactEntries = 128
const minCompactBytes = 4 << 20
const maxCheckpointSize = 128 << 20

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
	if c.Floor < p.floor {
		return 0, false, ErrCompacted
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
	if err := json.Unmarshal(log.Data, &c); err != nil {
		return applyResult{Err: ErrInvalid}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if c.Progress != nil {
		return p.applyProgress(log.Index, *c.Progress)
	}
	if !validCommand(c) {
		return applyResult{Err: ErrInvalid}
	}
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

func (p *partition) applyProgress(index uint64, update Progress) applyResult {
	if p.compact == nil || !p.members[update.Member] || update.Cursor >= index {
		return applyResult{Err: ErrInvalid}
	}
	if p.progress == nil {
		p.progress = make(map[string]uint64)
	}
	p.progress[update.Member] = max(p.progress[update.Member], update.Cursor)
	through := index
	for id := range p.members {
		through = min(through, p.progress[id])
	}
	if through <= p.floor {
		return applyResult{Index: index}
	}
	n := sort.Search(len(p.entries), func(i int) bool { return p.entries[i].Index > through })
	var reclaimed int
	for _, e := range p.entries[:n] {
		reclaimed += len(e.ID) + len(e.Payload)
	}
	if n < minCompactEntries && reclaimed < minCompactBytes {
		return applyResult{Index: index}
	}
	checkpoint, err := p.compact(p.checkpoint, p.entries[:n])
	if err != nil || len(checkpoint) == 0 || len(checkpoint) > maxCheckpointSize {
		p.compactFailed = true
		return applyResult{Index: index}
	}
	p.compactFailed = false
	p.floor = p.entries[n-1].Index
	p.checkpoint = checkpoint
	p.entries = append([]Entry(nil), p.entries[n:]...)
	p.ids = make(map[string]int, len(p.entries))
	for i, e := range p.entries {
		p.ids[e.ID] = i
	}
	p.bytes -= reclaimed
	return applyResult{Index: index}
}

func (p *partition) entriesAfter(after uint64, limit int) []Entry {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.entriesLocked(after, limit)
}

func (p *partition) entriesLocked(after uint64, limit int) []Entry {
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
	Version    int               `json:"version"`
	Entries    []Entry           `json:"entries"`
	Floor      uint64            `json:"floor,omitempty"`
	Checkpoint json.RawMessage   `json:"checkpoint,omitempty"`
	Progress   map[string]uint64 `json:"progress,omitempty"`
}
type partitionSnapshot struct{ data snapshot }

func (p *partition) Snapshot() (raft.FSMSnapshot, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	data := snapshot{Version: 1, Entries: append([]Entry(nil), p.entries...)}
	if p.compact != nil {
		data.Version, data.Floor, data.Checkpoint = 2, p.floor, p.checkpoint
		data.Progress = make(map[string]uint64, len(p.progress))
		for member, cursor := range p.progress {
			data.Progress[member] = cursor
		}
	}
	return &partitionSnapshot{data}, nil
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
	limit := int64(2*maxPartitionBytes + 1)
	if p.compact != nil {
		limit += maxCheckpointSize
	}
	limited := &io.LimitedReader{R: reader, N: limit}
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	var s snapshot
	if err := decoder.Decode(&s); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF || limited.N == 0 || len(s.Entries) > maxPartitionEntries {
		return ErrInvalid
	}
	switch s.Version {
	case 1:
		if s.Floor != 0 || len(s.Checkpoint) != 0 || len(s.Progress) != 0 {
			return ErrInvalid
		}
	case 2:
		if p.compact == nil || (s.Floor == 0) != (len(s.Checkpoint) == 0) || len(s.Checkpoint) > maxCheckpointSize {
			return ErrInvalid
		}
		for member, cursor := range s.Progress {
			if !p.members[member] || cursor < s.Floor {
				return ErrInvalid
			}
		}
		if s.Floor != 0 {
			if len(s.Progress) != len(p.members) {
				return ErrInvalid
			}
			if _, err := p.compact(s.Checkpoint, nil); err != nil {
				return err
			}
		}
	default:
		return ErrInvalid
	}
	ids := make(map[string]int, len(s.Entries))
	var size int
	previous := s.Floor
	for i, e := range s.Entries {
		if !validCommand(command{ID: e.ID, Payload: e.Payload}) || e.Index <= previous {
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
	p.floor, p.checkpoint, p.progress, p.compactFailed = s.Floor, s.Checkpoint, s.Progress, false
	p.mu.Unlock()
	return nil
}
