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
	// RaftConfig is copied; LocalID and LogOutput are supplied by this package.
	RaftConfig *raft.Config
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
	snapshots, err := raft.NewFileSnapshotStore(config.Dir, 2, io.Discard)
	if err != nil {
		store.Close()
		return nil, err
	}
	existing, err := raft.HasExistingState(store, store, snapshots)
	if err != nil {
		store.Close()
		return nil, err
	}
	identityKey := []byte("hmq/node-identity/v2")
	expectedIdentity, _ := json.Marshal(struct{ ID, Cluster string }{config.ID, config.ClusterID})
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
	fsm := &partition{}
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
	c := command{id, payload}
	if !validCommand(c) || timeout <= 0 {
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
	if err := n.fsm.preflight(c); err != nil {
		return 0, err
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

// Entries is a local applied-state view, not a linearizable read. A follower may
// lag. Callers must not interpret it as a quorum-confirmed read barrier.
func (n *Node) Entries(after uint64, limit int) ([]Entry, error) {
	if limit <= 0 || limit > 1024 {
		return nil, ErrInvalid
	}
	return n.fsm.entriesAfter(after, limit), nil
}
func (n *Node) Snapshot() error { return n.raft.Snapshot().Error() }
func (n *Node) Close() error {
	n.once.Do(func() { n.closeErr = errors.Join(n.raft.Shutdown().Error(), n.store.Close()) })
	return n.closeErr
}
