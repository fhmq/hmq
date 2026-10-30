package broker

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/v3/broker/replication"
	"github.com/fhmq/hmq/v3/broker/storage"
	"github.com/hashicorp/raft"
)

var ErrQuorumRoutingUnavailable = errors.New("quorum: unsupported publication or side effect")

func (b *Broker) quorumEnabled() bool {
	return b != nil && b.config != nil && b.config.Cluster.Quorum.Enabled
}

func validateQuorumConfig(config *Config) error {
	c, q := config.Cluster, config.Cluster.Quorum
	if !q.Enabled {
		if q.DataDir != "" || q.Listen != "" || len(q.Peers) != 0 {
			return errors.New("quorum settings require enabled mode")
		}
		return nil
	}
	if c.Reliable.Enabled {
		return errors.New("quorum and reliable modes are mutually exclusive")
	}
	if config.Plugin.Bridge != nil || config.ExperimentalSessionLogPath != "" || config.RetainedLogPath != "" {
		return errors.New("quorum does not support bridge, session log or retained storage")
	}
	if c.Listen == "" || !clusterNodeIDPattern.MatchString(c.NodeID) || strings.TrimSpace(c.ClusterID) == "" || strings.TrimSpace(c.Secret) == "" {
		return errors.New("quorum requires a configured cluster")
	}
	if c.TLS.CertFile == "" || c.TLS.KeyFile == "" || c.TLS.CAFile == "" {
		return errors.New("quorum requires cluster TLS")
	}
	if !validClusterAddress(q.Listen, false) || q.Listen == c.Listen {
		return errors.New("quorum requires a distinct valid listen address")
	}
	if len(q.Peers) == 0 || q.Peers[c.NodeID] != q.Listen {
		return errors.New("quorum peers must contain the local ID and listen address")
	}
	addresses := make(map[string]bool)
	for id, address := range q.Peers {
		if !clusterNodeIDPattern.MatchString(id) || !validClusterAddress(address, false) || addresses[address] {
			return errors.New("invalid quorum peer ID or duplicate/invalid address")
		}
		addresses[address] = true
	}
	if q.DataDir == "" || strings.TrimSpace(q.DataDir) != q.DataDir {
		return errors.New("quorum dataDir must be nonblank without surrounding whitespace")
	}
	dir, err := canonicalLogPath(q.DataDir)
	if err != nil {
		return fmt.Errorf("invalid quorum dataDir: %w", err)
	}
	if info, err := os.Stat(dir); err == nil && !info.IsDir() {
		return errors.New("quorum dataDir must be a directory")
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, path := range []string{config.RetainedLogPath, config.ExperimentalSessionLogPath, c.Reliable.EventLogPath, c.Reliable.InboxLogPath} {
		if path == "" {
			continue
		}
		other, err := canonicalLogPath(path)
		if err != nil {
			return err
		}
		// Exclude overlap in either direction, including aliases through symlinks.
		a, _ := filepath.Rel(dir, other)
		b, _ := filepath.Rel(other, dir)
		inside := func(p string) bool { return p != ".." && !strings.HasPrefix(p, ".."+string(filepath.Separator)) }
		if inside(a) || inside(b) {
			return errors.New("quorum dataDir must not overlap other storage paths")
		}
	}
	return nil
}

type quorumRuntime struct {
	forward    *quorumForward
	mu         sync.RWMutex
	node       *replication.Node
	transport  *raft.NetworkTransport
	closed     bool
	proposals  chan struct{}
	proposalWG sync.WaitGroup
	cursorLog  *storage.Log
	cursor     uint64
	consumeErr error
	stop       chan struct{}
	done       chan struct{}
	closeOnce  sync.Once
	closeErr   error
	started    bool
}

type quorumCursorRecord struct {
	Version int    `json:"version"`
	ID      string `json:"id"`
	Cluster string `json:"cluster"`
	Index   uint64 `json:"index"`
}

func openQuorumCursor(dir, id, clusterID string) (*storage.Log, uint64, error) {
	l, err := storage.Open(filepath.Join(dir, "delivery.wal"))
	if err != nil {
		return nil, 0, err
	}
	var index uint64
	first := true
	err = l.Replay(func(data []byte) error {
		var record quorumCursorRecord
		if err := stdjson.Unmarshal(data, &record); err != nil {
			return err
		}
		if record.Version != 1 || record.ID != id || record.Cluster != clusterID || (first && record.Index != 0) || (!first && record.Index <= index) {
			return errors.New("quorum: invalid delivery cursor identity or order")
		}
		first, index = false, record.Index
		return nil
	})
	if err == nil && first {
		data, _ := stdjson.Marshal(quorumCursorRecord{Version: 1, ID: id, Cluster: clusterID})
		err = l.Append(data)
	}
	if err != nil {
		_ = l.Close()
		return nil, 0, err
	}
	return l, index, nil
}

func openQuorumRuntime(c RouteInfo, serverTLS, clientTLS *tls.Config) (*quorumRuntime, error) {
	dir, err := canonicalLogPath(c.Quorum.DataDir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	// Never bootstrap a fresh Raft history beside an old delivery cursor, or
	// silently replay history whose cursor disappeared. Partial creation fails closed.
	var exists [2]bool
	for i, name := range []string{"raft.wal", "delivery.wal"} {
		info, err := os.Stat(filepath.Join(dir, name))
		if err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		if err == nil && (!info.Mode().IsRegular() || info.Size() == 0) {
			return nil, errors.New("quorum: incomplete or invalid WAL file; restore complete node state")
		}
		exists[i] = err == nil
	}
	if exists[0] != exists[1] {
		return nil, errors.New("quorum: incomplete raft/delivery WAL pair; restore complete node state")
	}
	layer, err := replication.ListenTLS(replication.TLSStreamConfig{ID: c.NodeID, ClusterID: c.ClusterID, Listen: c.Quorum.Listen, Peers: c.Quorum.Peers, ServerTLS: serverTLS, ClientTLS: clientTLS, Timeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}
	transport := raft.NewNetworkTransport(layer, 3, 5*time.Second, io.Discard)
	ids := make([]string, 0, len(c.Quorum.Peers))
	for id := range c.Quorum.Peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	servers := make([]raft.Server, 0, len(ids))
	for _, id := range ids {
		servers = append(servers, raft.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(c.Quorum.Peers[id]), Suffrage: raft.Voter})
	}
	node, err := replication.Open(replication.Config{ID: c.NodeID, ClusterID: c.ClusterID, Dir: dir, Transport: transport, Bootstrap: servers})
	if err != nil {
		_ = transport.Close()
		return nil, err
	}
	cursorLog, cursor, err := openQuorumCursor(dir, c.NodeID, c.ClusterID)
	if err != nil {
		_ = closeQuorumNetwork(node, transport)
		return nil, err
	}
	if cursor > node.LastIndex() {
		_ = cursorLog.Close()
		_ = closeQuorumNetwork(node, transport)
		return nil, errors.New("quorum: delivery cursor exceeds local Raft log/snapshot upper bound")
	}
	return &quorumRuntime{node: node, transport: transport, cursorLog: cursorLog, cursor: cursor, stop: make(chan struct{}), done: make(chan struct{}), proposals: make(chan struct{}, quorumProposalLimit)}, nil
}

func (r *quorumRuntime) BeginClose() {
	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	if r.forward != nil {
		r.forward.BeginClose()
	}
}

func (r *quorumRuntime) Close() error {
	r.BeginClose()
	r.closeOnce.Do(func() {
		r.mu.Lock()
		r.closed = true
		started := r.started
		close(r.stop)
		r.mu.Unlock()
		networkDone := make(chan error, 1)
		go func() { networkDone <- closeQuorumNetwork(r.node, r.transport) }()
		if started {
			<-r.done
		}
		r.proposalWG.Wait()
		if r.forward != nil {
			r.forward.wg.Wait()
		}
		r.closeErr = errors.Join(<-networkDone, r.cursorLog.Close())
	})
	return r.closeErr
}

// Shutdown must abort size-scaled snapshot RPCs while Raft waits for its workers.
// Node.Close alone owns the store and closes it only after Raft shutdown finishes.
func closeQuorumNetwork(node, transport io.Closer) error {
	nodeDone := make(chan error, 1)
	go func() { nodeDone <- node.Close() }()
	transportErr := transport.Close()
	return errors.Join(<-nodeDone, transportErr)
}

func validQuorumPublication(p *packets.PublishPacket) bool {
	return p != nil && len(p.Payload) <= replication.MaxPayloadSize && p.Qos <= 1 && !p.Retain && p.TopicName != "" && len(p.TopicName) <= 65535 && utf8.ValidString(p.TopicName) && !strings.ContainsAny(p.TopicName, "+#\x00") && !strings.HasPrefix(p.TopicName, "$")
}

const quorumProposalLimit = 128

// Closing admission and registering in-flight work share mu, so shutdown's
// Wait cannot race a new Add after the final outstanding proposal completes.
func (r *quorumRuntime) acquireProposal() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return storage.ErrClosed
	}
	if r.consumeErr != nil {
		return errors.New("quorum: local delivery consumer unhealthy")
	}
	select {
	case r.proposals <- struct{}{}:
		r.proposalWG.Add(1)
		return nil
	default:
		return replication.ErrBusy
	}
}
func (r *quorumRuntime) releaseProposal() {
	<-r.proposals
	r.proposalWG.Done()
}

func (r *quorumRuntime) publish(p *packets.PublishPacket) error {
	if !validQuorumPublication(p) {
		return ErrQuorumRoutingUnavailable
	}
	if err := r.acquireProposal(); err != nil {
		return err
	}
	defer r.releaseProposal()
	data, err := stdjson.Marshal(clusterPublish{p.TopicName, p.Payload, p.Qos, false})
	if err != nil {
		return err
	}
	if len(data) > replication.MaxPayloadSize {
		return ErrQuorumRoutingUnavailable
	}
	if r.forward != nil {
		return r.forward.publish(data)
	}
	if r.node.State() != raft.Leader {
		return raft.ErrNotLeader
	}
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return err
	}
	index, err := r.node.Publish(hex.EncodeToString(id[:]), data, 5*time.Second)
	if err == nil && index == 0 {
		return errors.New("quorum: zero commit index")
	}
	return err
}

func (c *client) processQuorumPublish(p *packets.PublishPacket) {
	c.publishMu.Lock()
	defer c.publishMu.Unlock()
	if c.typ != CLIENT || !validQuorumPublication(p) || (p.Qos == 1 && p.MessageID == 0) || !c.broker.CheckTopicAuth(PUB, c.info.clientID, c.info.username, c.info.remoteIP, p.TopicName) {
		c.Close()
		return
	}
	// No bridge, retained write or local delivery may precede this commit.
	if err := c.broker.quorumRuntime.publish(p); err != nil {
		c.Close()
		return
	}
	if p.Qos == 1 {
		ack := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
		ack.MessageID = p.MessageID
		if err := c.WriterPacket(ack); err != nil {
			c.Close()
		}
	}
}

// Each replica consumes only applied (committed) entries, never legacy frames.
// Delivery precedes the fsync cursor: a crash can repeat a partial/full delivery.
func (r *quorumRuntime) startConsumer(id, clusterID string, deliver func(*packets.PublishPacket) error) {
	r.mu.Lock()
	if r.started || r.closed {
		r.mu.Unlock()
		return
	}
	r.started = true
	r.mu.Unlock()
	go func() {
		defer close(r.done)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.stop:
				return
			default:
			}
			r.mu.RLock()
			after := r.cursor
			r.mu.RUnlock()
			entries, err := r.node.Entries(after, 64)
			for _, entry := range entries {
				select {
				case <-r.stop:
					return
				default:
				}
				var msg clusterPublish
				if err = stdjson.Unmarshal(entry.Payload, &msg); err != nil {
					break
				}
				p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
				p.TopicName, p.Payload, p.Qos, p.Retain = msg.Topic, msg.Payload, msg.QoS, msg.Retain
				if !validQuorumPublication(p) {
					err = ErrQuorumRoutingUnavailable
					break
				}
				if err = deliver(p); err != nil {
					break
				}
				data, _ := stdjson.Marshal(quorumCursorRecord{1, id, clusterID, entry.Index})
				if err = r.cursorLog.Append(data); err != nil {
					r.mu.Lock()
					r.consumeErr = err
					r.mu.Unlock()
					return // WAL errors are poisoned; reopening is required.
				}
				r.mu.Lock()
				r.cursor = entry.Index
				r.mu.Unlock()
			}
			r.mu.Lock()
			r.consumeErr = err
			r.mu.Unlock()
			select {
			case <-r.stop:
				return
			case <-ticker.C:
			}
		}
	}()
}

type quorumHealth struct {
	State          string `json:"state"`
	Partitions     int    `json:"partitions"`
	Routing        string `json:"routing"`
	LeaderID       string `json:"leaderID,omitempty"`
	Cursor         uint64 `json:"cursor"`
	ConsumerFailed bool   `json:"consumerFailed"`
	ActiveRequests int    `json:"activeRequests"`
	ProposalLimit  int    `json:"proposalLimit"`
}

func (r *quorumRuntime) health() quorumHealth {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h := quorumHealth{State: "closed", Partitions: 1, Routing: "quorum_qos01_nonretained", Cursor: r.cursor, ConsumerFailed: r.consumeErr != nil}
	h.ActiveRequests, h.ProposalLimit = len(r.proposals), cap(r.proposals)
	if !r.closed {
		h.State = r.node.State().String()
		_, id := r.node.Leader()
		h.LeaderID = string(id)
	}
	return h
}
