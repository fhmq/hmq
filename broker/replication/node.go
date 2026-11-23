// Package replication provides an embedded single-partition Raft kernel.
// Broker routing integration is separate from its consensus and transport APIs.
package replication

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/hashicorp/raft"
)

type Config struct {
	ID        string
	ClusterID string
	Dir       string
	Transport raft.Transport
	Bootstrap []raft.Server
	// Application distinguishes incompatible state-machine profiles.
	Application string
	// RaftConfig is copied; LocalID and LogOutput are supplied by this package.
	RaftConfig *raft.Config
	// Checkpoint folds a committed prefix into an immutable JSON checkpoint.
	// Its inputs are read-only; it must be deterministic across members.
	Checkpoint func([]byte, []Entry) ([]byte, error)
}
type Node struct {
	raft     *raft.Raft
	store    *Store
	fsm      *partition
	once     sync.Once
	closeErr error
	pending  chan struct{}
}

var ErrBusy = errors.New("replication: pending proposal limit reached")
var ErrTimeout = errors.New("replication: proposal outcome uncertain after timeout")
var ErrIdentity = errors.New("replication: data directory belongs to another node or is unbound")

const maxAppendBatch = 4

// Open requires an existing directory. The caller owns the transport and keeps
// it alive during normal operation. It may close it concurrently with Close to
// abort stalled RPCs during shutdown. Bootstrap is only applied to empty state.
func Open(config Config) (*Node, error) {
	if config.ID == "" || len(config.ID) > 256 || !utf8.ValidString(config.ID) || config.Dir == "" || config.Transport == nil {
		return nil, ErrInvalid
	}
	settings := raft.DefaultConfig()
	if config.RaftConfig != nil {
		copy := *config.RaftConfig
		settings = &copy
	}
	settings.LocalID = raft.ServerID(config.ID)
	settings.LogOutput = io.Discard
	// Commands and WAL records each encode payloads. Four maximum-sized
	// commands leave headroom below storage's 16 MiB atomic record bound.
	if settings.MaxAppendEntries > maxAppendBatch {
		settings.MaxAppendEntries = maxAppendBatch
	}
	if err := raft.ValidateConfig(settings); err != nil {
		return nil, err
	}
	store, err := OpenStore(filepath.Join(config.Dir, "raft.wal"))
	if err != nil {
		return nil, err
	}
	files, err := raft.NewFileSnapshotStore(config.Dir, 2, io.Discard)
	if err != nil {
		store.Close()
		return nil, err
	}
	var snapshots raft.SnapshotStore = files
	if config.Checkpoint != nil {
		snapshots = checkpointSnapshots{files, store}
	}
	existing, err := raft.HasExistingState(store, store, snapshots)
	if err != nil {
		store.Close()
		return nil, err
	}
	identityKey := []byte("hmq/node-identity/v2")
	expectedIdentity, _ := json.Marshal(struct {
		ID, Cluster string
		Application string `json:",omitempty"`
	}{config.ID, config.ClusterID, config.Application})
	identity, err := store.Get(identityKey)
	if err != nil {
		store.Close()
		return nil, err
	}
	if len(identity) == 0 {
		if existing {
			store.Close()
			return nil, ErrIdentity
		}
		if err := store.Set(identityKey, expectedIdentity); err != nil {
			store.Close()
			return nil, err
		}
	} else if !bytes.Equal(identity, expectedIdentity) {
		store.Close()
		return nil, ErrIdentity
	}
	if !existing && len(config.Bootstrap) > 0 {
		if err := raft.BootstrapCluster(settings, store, store, snapshots, config.Transport, raft.Configuration{Servers: config.Bootstrap}); err != nil {
			store.Close()
			return nil, err
		}
	}
	fsm := &partition{compact: config.Checkpoint}
	if config.Checkpoint != nil {
		fsm.members = make(map[string]bool, len(config.Bootstrap))
		for _, member := range config.Bootstrap {
			fsm.members[string(member.ID)] = true
		}
	}
	r, err := raft.NewRaft(settings, fsm, store, store, snapshots, config.Transport)
	if err != nil {
		store.Close()
		return nil, err
	}
	return &Node{raft: r, store: store, fsm: fsm, pending: make(chan struct{}, 128)}, nil
}

// Publish reports success only after quorum commit and local FSM application.
// A timeout/lost leadership is an uncertain outcome; retry the same ID/payload.
func (n *Node) Publish(id string, payload []byte, timeout time.Duration) (uint64, error) {
	return n.PublishAt(id, payload, 0, timeout)
}

func (n *Node) PublishAt(id string, payload []byte, floor uint64, timeout time.Duration) (uint64, error) {
	c := command{ID: id, Payload: payload, Floor: floor}
	if !validCommand(c) {
		return 0, ErrInvalid
	}
	return n.propose(c, timeout)
}

func (n *Node) ReportProgress(update Progress, timeout time.Duration) (uint64, error) {
	if !n.fsm.members[update.Member] || update.Cursor == 0 {
		return 0, ErrInvalid
	}
	return n.propose(command{Progress: &update}, timeout)
}

func (n *Node) propose(c command, timeout time.Duration) (uint64, error) {
	if timeout <= 0 {
		return 0, ErrInvalid
	}
	deadline := time.Now().Add(timeout)
	state := n.raft.State()
	if state == raft.Shutdown {
		return 0, raft.ErrRaftShutdown
	}
	if state != raft.Leader {
		return 0, raft.ErrNotLeader
	}
	if c.Progress == nil {
		if err := n.fsm.preflight(c); err != nil {
			return 0, err
		}
	}
	select {
	case n.pending <- struct{}{}:
	default:
		return 0, ErrBusy
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	data, err := json.Marshal(c)
	if err != nil {
		<-n.pending
		return 0, err
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		<-n.pending
		return 0, ErrTimeout
	}
	future := n.raft.Apply(data, remaining)
	result := make(chan applyResult, 1)
	go func() {
		defer func() { <-n.pending }()
		if err := future.Error(); err != nil {
			result <- applyResult{Err: err}
			return
		}
		r, ok := future.Response().(applyResult)
		if !ok {
			r = applyResult{Err: errors.New("replication: unexpected FSM response")}
		}
		result <- r
	}()
	select {
	case r := <-result:
		if !time.Now().Before(deadline) {
			return 0, ErrTimeout
		}
		return r.Index, r.Err
	case <-timer.C:
		return 0, ErrTimeout
	}
}
func (n *Node) State() raft.RaftState                       { return n.raft.State() }
func (n *Node) Leader() (raft.ServerAddress, raft.ServerID) { return n.raft.LeaderWithID() }

// LastIndex is a local log/snapshot bound, not proof of quorum commitment.
func (n *Node) LastIndex() uint64 { return n.raft.LastIndex() }

type Capacity struct {
	Entries          int    `json:"entries"`
	Bytes            int    `json:"bytes"`
	MaxEntries       int    `json:"maxEntries"`
	MaxBytes         int    `json:"maxBytes"`
	CompactedThrough uint64 `json:"compactedThrough"`
	CompactionFailed bool   `json:"compactionFailed"`
}

// Capacity describes local retained application history, not Raft WAL disk use.
func (n *Node) Capacity() Capacity {
	n.fsm.mu.RLock()
	defer n.fsm.mu.RUnlock()
	return Capacity{len(n.fsm.entries), n.fsm.bytes, maxPartitionEntries, maxPartitionBytes, n.fsm.floor, n.fsm.compactFailed}
}

func (n *Node) Floor() uint64 {
	n.fsm.mu.RLock()
	defer n.fsm.mu.RUnlock()
	return n.fsm.floor
}

// HistoryTip is the latest locally applied application entry or checkpoint.
func (n *Node) HistoryTip() uint64 {
	p := n.fsm
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.entries) == 0 {
		return p.floor
	}
	return p.entries[len(p.entries)-1].Index
}

// History returns one consistent view; consumers behind the floor must first
// restore its checkpoint, then replay the suffix in order.
func (n *Node) History(after uint64, limit int) (uint64, []byte, []Entry) {
	p := n.fsm
	p.mu.RLock()
	defer p.mu.RUnlock()
	var checkpoint []byte
	if after < p.floor {
		checkpoint = bytes.Clone(p.checkpoint)
	}
	return p.floor, checkpoint, p.entriesLocked(after, limit)
}

// Entries is a local applied-state view, not a linearizable read. A follower may
// lag. Callers must not interpret it as a quorum-confirmed read barrier.
func (n *Node) Entries(after uint64, limit int) ([]Entry, error) {
	if limit <= 0 || limit > 1024 {
		return nil, ErrInvalid
	}
	p := n.fsm
	p.mu.RLock()
	defer p.mu.RUnlock()
	if after < p.floor {
		return nil, ErrCompacted
	}
	return p.entriesLocked(after, limit), nil
}
func (n *Node) Snapshot() error { return n.raft.Snapshot().Error() }
func (n *Node) CheckpointWAL() error {
	if err := n.Snapshot(); err != nil && !errors.Is(err, raft.ErrNothingNewToSnapshot) {
		return err
	}
	return n.store.compact()
}
func (n *Node) Close() error {
	n.once.Do(func() { n.closeErr = errors.Join(n.raft.Shutdown().Error(), n.store.Close()) })
	return n.closeErr
}
