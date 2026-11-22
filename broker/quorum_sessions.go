package broker

import (
	stdjson "encoding/json"
	"sync"
	"time"

	"github.com/fhmq/hmq/v4/broker/packets"
	"github.com/fhmq/hmq/v4/broker/replication"
	"github.com/fhmq/hmq/v4/broker/sessionstore"
)

// SessionStorage requires durable CAS mutations, epoch fencing, and an atomic
// Release that completes the inbound exchange and enqueues durable deliveries.
type SessionStorage interface {
	Get(string) (*sessionstore.State, uint64, error)
	Put(string, uint64, *sessionstore.State) (uint64, error)
	Apply(string, uint64, sessionstore.Batch) (uint64, error)
	Inspect(string, uint16) (uint64, bool, sessionstore.Inflight, bool, error)
	List(string, int, int) ([]string, uint64, error)
	ReadQueue(string, int, int, int) ([]sessionstore.Message, uint64, error)
	ReadSubscriptions(string, string, int, int) ([]sessionstore.Subscription, uint64, error)
	ReadInflight(string, uint64, int, int) ([]sessionstore.InflightEntry, uint64, error)
	Receive(string, uint64, uint16, sessionstore.Message, int) error
	Release(string, uint64, uint16, time.Time) (sessionstore.Message, bool, error)
	Close() error
}

type quorumSessionCommand struct {
	SessionOp string
	ID        string                `json:",omitempty"`
	Version   uint64                `json:",omitempty"`
	PacketID  uint16                `json:",omitempty"`
	Maximum   int                   `json:",omitempty"`
	State     *sessionstore.State   `json:",omitempty"`
	Batch     *sessionstore.Batch   `json:",omitempty"`
	Message   *sessionstore.Message `json:",omitempty"`
	Time      *time.Time            `json:",omitempty"`
	Quota     *sessionstore.Limits  `json:",omitempty"`
}

func (c *quorumSessionCommand) quota() sessionstore.Limits {
	if c.Quota != nil {
		return *c.Quota
	}
	return sessionstore.Limits{}
}

func decodeQuorumSession(data []byte) *quorumSessionCommand {
	var c quorumSessionCommand
	if stdjson.Unmarshal(data, &c) != nil || !c.quota().Valid() {
		return nil
	}
	switch c.SessionOp {
	case "publish":
		if c.Message == nil || c.Message.QoS != 1 ||
			!validQuorumPublication(&packets.PublishPacket{FixedHeader: packets.FixedHeader{Qos: 1, Retain: c.Message.Retain}, TopicName: c.Message.Topic, Payload: c.Message.Payload}) {
			return nil
		}
		return &c
	case "barrier":
		return &c
	case "put", "apply", "receive", "release":
		if c.ID == "" {
			return nil
		}
		if c.SessionOp == "apply" && c.Batch == nil ||
			c.SessionOp == "receive" && (c.Message == nil || c.PacketID == 0 || c.Message.Retain || c.Message.QoS != 2) ||
			c.SessionOp == "release" && (c.Time == nil || c.PacketID == 0) {
			return nil
		}
		return &c
	default:
		return nil
	}
}

type quorumSessionResult struct {
	version uint64
	message sessionstore.Message
	found   bool
	err     error
}

// The local view is rebuilt from applied Raft entries, including entries
// retained by a snapshot. No independently writable session WAL exists here.
type quorumSessions struct {
	*sessionstore.Store
	r       *quorumRuntime
	limits  sessionstore.Limits
	mu      sync.Mutex
	index   uint64
	results map[uint64]quorumSessionResult
}

func (s *quorumSessions) advance(through uint64) (quorumSessionResult, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.index < through {
		entries, err := s.r.node.Entries(s.index, 64)
		if err != nil || len(entries) == 0 {
			return quorumSessionResult{}, false, err
		}
		for _, e := range entries {
			if e.Index > through {
				return quorumSessionResult{}, false, replication.ErrInvalid
			}
			if cmd := decodeQuorumSession(e.Payload); cmd != nil {
				var r quorumSessionResult
				switch cmd.SessionOp {
				case "put":
					r.version, r.err = s.Store.Put(cmd.ID, cmd.Version, cmd.State)
				case "apply":
					r.version, r.err = s.Store.ApplyLimited(cmd.ID, cmd.Version, *cmd.Batch, cmd.quota())
				case "receive":
					r.err = s.Store.ReceiveLimited(cmd.ID, cmd.Version, cmd.PacketID, *cmd.Message, cmd.Maximum, cmd.quota())
				case "release":
					r.message, r.found, r.err = s.Store.ReleaseLimited(cmd.ID, cmd.Version, cmd.PacketID, *cmd.Time, cmd.quota())
				case "publish":
					r.err = s.Store.Fanout(*cmd.Message, cmd.quota())
					if r.err == nil {
						r.message = *cmd.Message
					}
				}
				s.results[e.Index] = r
			} else {
				var p clusterPublish
				if err := stdjson.Unmarshal(e.Payload, &p); err != nil || p.Topic == "" {
					return quorumSessionResult{}, false, replication.ErrInvalid
				}
				if err := s.Store.Fanout(sessionstore.Message{Topic: p.Topic, Payload: p.Payload, QoS: p.QoS, Metadata: p.Metadata, QueuedAt: p.QueuedAt}, sessionstore.Limits{}); err != nil {
					return quorumSessionResult{}, false, err
				}
			}
			s.index = e.Index
			if e.Index == through {
				break
			}
		}
	}
	return s.results[through], true, nil
}

func (s *quorumSessions) commit(c quorumSessionCommand) (quorumSessionResult, error) {
	if s.limits.Enabled() {
		c.Quota = &s.limits
	}
	data, err := stdjson.Marshal(c)
	if err != nil {
		return quorumSessionResult{}, err
	}
	if len(data) > replication.MaxPayloadSize || decodeQuorumSession(data) == nil {
		return quorumSessionResult{}, replication.ErrInvalid
	}
	deadline := time.Now().Add(6 * time.Second)
	index, err := s.r.forward.commit(data)
	if err != nil {
		return quorumSessionResult{}, err
	}
	for {
		result, applied, err := s.advance(index)
		if err != nil {
			return result, err
		}
		if applied {
			return result, result.err
		}
		if time.Now().After(deadline) {
			return quorumSessionResult{}, replication.ErrTimeout
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (s *quorumSessions) Put(id string, version uint64, state *sessionstore.State) (uint64, error) {
	r, err := s.commit(quorumSessionCommand{SessionOp: "put", ID: id, Version: version, State: state})
	return r.version, err
}

func (s *quorumSessions) Apply(id string, version uint64, batch sessionstore.Batch) (uint64, error) {
	r, err := s.commit(quorumSessionCommand{SessionOp: "apply", ID: id, Version: version, Batch: &batch})
	return r.version, err
}

func (s *quorumSessions) Receive(id string, epoch uint64, packetID uint16, message sessionstore.Message, max int) error {
	_, err := s.commit(quorumSessionCommand{SessionOp: "receive", ID: id, Version: epoch, PacketID: packetID, Message: &message, Maximum: max})
	return err
}

func (s *quorumSessions) Release(id string, epoch uint64, packetID uint16, now time.Time) (sessionstore.Message, bool, error) {
	r, err := s.commit(quorumSessionCommand{SessionOp: "release", ID: id, Version: epoch, PacketID: packetID, Time: &now})
	return r.message, r.found, err
}
