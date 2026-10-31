package broker

import (
	"errors"
	"github.com/fhmq/hmq/v4/broker/lib/sessions"
	"github.com/fhmq/hmq/v4/broker/lib/topics"
	"github.com/fhmq/hmq/v4/broker/packets"
	"github.com/fhmq/hmq/v4/broker/sessionstore"
	"net"
	"strings"
	"sync"
	"time"
)

type durableFilter struct {
	id, filter string
	qos        byte
}
type durableIndex struct {
	mu      sync.RWMutex
	matcher *topics.Manager
	filters map[string]map[string]*durableFilter
}

func (b *Broker) initSessionIndex() error {
	m, err := topics.NewManager("mem")
	if err != nil {
		return err
	}
	b.sessionIndex = &durableIndex{matcher: m, filters: make(map[string]map[string]*durableFilter)}
	after := ""
	for {
		ids, _, err := b.sessionLog.List(after, 128, 1<<20)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		for _, id := range ids {
			afterFilter := ""
			var version uint64
			first := true
			for {
				filters, currentVersion, err := b.sessionLog.ReadSubscriptions(id, afterFilter, 128, 1<<20)
				if err != nil {
					return err
				}
				// Startup owns the store; a changed version indicates misuse rather
				// than a snapshot that can safely be merged into the routing index.
				if !first && currentVersion != version {
					return sessionstore.ErrConflict
				}
				version, first = currentVersion, false
				if len(filters) == 0 {
					break
				}
				for _, filter := range filters {
					q := filter.QoS
					if err := b.indexFilter(id, filter.Filter, &q); err != nil {
						return err
					}
				}
				afterFilter = filters[len(filters)-1].Filter
			}
		}
		after = ids[len(ids)-1]
	}
}
func (b *Broker) indexFilter(id, filter string, qos *byte) error {
	i := b.sessionIndex
	i.mu.Lock()
	defer i.mu.Unlock()
	if old := i.filters[id][filter]; old != nil {
		if err := i.matcher.Unsubscribe([]byte(filter), old); err != nil {
			return err
		}
		delete(i.filters[id], filter)
	}
	if qos == nil {
		return nil
	}
	if strings.HasPrefix(filter, "$share/") {
		return errors.New("durable shared subscriptions unsupported")
	}
	f := &durableFilter{id: id, filter: filter, qos: *qos}
	if _, err := i.matcher.Subscribe([]byte(filter), *qos, f); err != nil {
		return err
	}
	if i.filters[id] == nil {
		i.filters[id] = make(map[string]*durableFilter)
	}
	i.filters[id][filter] = f
	return nil
}
func (b *Broker) deleteSessionIndex(id string) {
	i := b.sessionIndex
	i.mu.Lock()
	defer i.mu.Unlock()
	for f, s := range i.filters[id] {
		_ = i.matcher.Unsubscribe([]byte(f), s)
	}
	delete(i.filters, id)
}
func (c *client) durable() bool {
	return c.typ == CLIENT && c.broker != nil && c.broker.sessionLog != nil && c.session != nil && !c.session.CleanSession()
}

// Never recreate a missing session or retry across a clean deletion.
func (b *Broker) applySession(id string, batch sessionstore.Batch) error {
	v, exists, _, _, err := b.sessionLog.Inspect(id, 0)
	if err != nil {
		return err
	}
	if !exists {
		return sessionstore.ErrConflict
	}
	_, err = b.sessionLog.Apply(id, v, batch)
	return err
}
func (c *client) persistFilter(filter string, qos *byte) error {
	if !c.durable() {
		return nil
	}
	batch := sessionstore.Batch{RemoveSubscriptions: []string{filter}}
	if qos != nil {
		batch = sessionstore.Batch{SetSubscriptions: map[string]byte{filter: *qos}}
	}
	batch.ExpectedEpoch = &c.durableEpoch
	if err := c.broker.applySession(c.info.clientID, batch); err != nil {
		return err
	}
	return c.broker.indexFilter(c.info.clientID, filter, qos)
}
func (c *client) persistOutbound(id uint16, msg *sessions.OutboundMessage) error {
	if !c.durable() {
		return nil
	}
	v, exists, item, found, err := c.broker.sessionLog.Inspect(c.info.clientID, id)
	if err != nil {
		return err
	}
	if !exists {
		return sessionstore.ErrConflict
	}
	batch := sessionstore.Batch{}
	batch.ExpectedEpoch = &c.durableEpoch
	if msg == nil {
		if !found {
			return nil
		}
		ack := "PUBACK"
		if item.Message.QoS == 2 {
			ack = "PUBCOMP"
		}
		batch.Acks = []sessionstore.Ack{{PacketID: id, Type: ack}}
	} else if msg.AwaitingPubcomp {
		if found && item.Phase == sessionstore.PubRel {
			return nil
		}
		batch.Acks = []sessionstore.Ack{{PacketID: id, Type: "PUBREC"}}
	} else {
		batch.Start = map[uint16]sessionstore.Message{id: {Topic: msg.Packet.TopicName, Payload: msg.Packet.Payload, QoS: msg.Packet.Qos, Retain: msg.Packet.Retain, Metadata: packets.CloneMetadata(msg.Packet.Metadata)}}
	}
	_, err = c.broker.sessionLog.Apply(c.info.clientID, v, batch)
	return err
}

// One index match; online/offline decisions and durable acceptance share the
// client-ID boundary. A disconnect before inflight registration falls back to
// enqueue; a disconnect after registration recovers the durable inflight.
func (b *Broker) routeDurable(packet *packets.PublishPacket) error {
	if b.sessionIndex == nil {
		return nil
	}
	i := b.sessionIndex
	var matches []interface{}
	var qs []byte
	i.mu.RLock()
	err := i.matcher.Subscribers([]byte(packet.TopicName), packet.Qos, &matches, &qs)
	i.mu.RUnlock()
	if err != nil {
		return err
	}
	byID := make(map[string][]*durableFilter)
	for _, m := range matches {
		f := m.(*durableFilter)
		byID[f.id] = append(byID[f.id], f)
	}
	for id, filters := range byID {
		unlock := b.lockClientConnect(id)
		err := func() error {
			i.mu.RLock()
			valid := false
			var qos byte
			for _, f := range filters {
				if i.filters[id][f.filter] == f {
					valid = true
					if f.qos > qos {
						qos = f.qos
					}
				}
			}
			i.mu.RUnlock()
			if !valid {
				return nil
			}
			if qos > packet.Qos {
				qos = packet.Qos
			}
			if value, ok := b.clients.Load(id); ok {
				c := value.(*client)
				if !c.isDisconnected() && c.durable() {
					if b.durableRouteHook != nil {
						b.durableRouteHook(id, "online")
					}
					err := publish(&subscription{client: c, qos: qos}, packet, qos)
					if errors.Is(err, errDurableAccepted) {
						return nil
					}
					if err == nil {
						return nil
					}
					if !errors.Is(err, errWriteNotStarted) {
						if errors.Is(err, net.ErrClosed) || errors.Is(err, errTakeoverWrite) {
							return nil
						}
						return err
					}
				}
			}
			if qos == 0 {
				return nil
			}
			return b.applySession(id, sessionstore.Batch{Enqueue: []sessionstore.Message{{Topic: packet.TopicName, Payload: packet.Payload, QoS: qos, Metadata: packets.CloneMetadata(packet.Metadata)}}})
		}()
		unlock()
		if err != nil {
			return err
		}
	}
	return nil
}
func (c *client) loadSessionLog() error {
	if !c.durable() {
		return nil
	}
	_, exists, _, _, err := c.broker.sessionLog.Inspect(c.info.clientID, 0)
	if err != nil {
		return err
	}
	if !exists {
		return sessionstore.ErrConflict
	}
	var version uint64
	first := true
	checkVersion := func(v uint64) error {
		if !first && version != v {
			return sessionstore.ErrConflict
		}
		version, first = v, false
		return nil
	}
	afterFilter := ""
	for {
		filters, v, err := c.broker.sessionLog.ReadSubscriptions(c.info.clientID, afterFilter, 128, 1<<20)
		if err != nil {
			return err
		}
		if err := checkVersion(v); err != nil {
			return err
		}
		if len(filters) == 0 {
			break
		}
		for _, filter := range filters {
			if err := c.session.AddTopic(filter.Filter, filter.QoS); err != nil {
				return err
			}
		}
		afterFilter = filters[len(filters)-1].Filter
	}
	var afterSequence uint64
	for {
		entries, v, err := c.broker.sessionLog.ReadInflight(c.info.clientID, afterSequence, 128, int(^uint(0)>>1))
		if err != nil {
			return err
		}
		if err := checkVersion(v); err != nil {
			return err
		}
		if len(entries) == 0 {
			break
		}
		for _, entry := range entries {
			id, item := entry.PacketID, entry.Inflight
			p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			p.TopicName, p.Payload, p.Qos, p.MessageID = item.Message.Topic, item.Message.Payload, item.Message.QoS, id
			p.Retain = item.Message.Retain
			p.Metadata = packets.CloneMetadata(item.Message.Metadata)
			c.session.SetOutbound(id, sessions.OutboundMessage{Packet: p, AwaitingPubcomp: item.Phase == sessionstore.PubRel})
			c.durableReplayIDs = append(c.durableReplayIDs, id)
			afterSequence = item.Sequence
		}
	}
	return nil
}

// The caller holds the client-ID lock through commit, CONNACK, and owner claim.
func (c *client) prepareSessionLog(clean bool) (func() error, error) {
	b := c.broker
	id := c.info.clientID
	s, v, err := b.sessionLog.Get(id)
	if err != nil {
		return nil, err
	}
	saved := s
	rollback := func() error {
		current, version, err := b.sessionLog.Get(id)
		if err != nil {
			return err
		}
		if saved != nil && current != nil && saved.LastSequence < current.LastSequence {
			saved.LastSequence = current.LastSequence
		}
		_, err = b.sessionLog.Put(id, version, saved)
		return err
	}
	if clean {
		_, err = b.sessionLog.Put(id, v, nil)
		return rollback, err
	}
	epoch := v + 1 // Tombstone versions prevent epoch reuse after clean deletion.
	if epoch == 0 {
		return nil, sessionstore.ErrInvalid
	}
	if s == nil {
		s = &sessionstore.State{Epoch: epoch}
		v, err = b.sessionLog.Put(id, v, s)
	} else {
		if epoch <= s.Epoch {
			epoch = s.Epoch + 1
		}
		if epoch == 0 {
			return nil, sessionstore.ErrInvalid
		}
		v, err = b.sessionLog.Apply(id, v, sessionstore.Batch{NextEpoch: &epoch})
		c.durableEpoch = epoch
	}
	if err != nil {
		return rollback, err
	}
	if c.durableEpoch == 0 {
		c.durableEpoch = s.Epoch
	}
	for filter := range s.Subscriptions {
		if strings.HasPrefix(filter, "$share/") {
			return rollback, errors.New("durable shared subscriptions unsupported")
		}
	}
	used := make(map[uint16]bool)
	for id := range s.Inflight {
		used[id] = true
	}
	remaining := b.durableWindow() - len(s.Inflight)
	for _, message := range s.Queue {
		if metadata := message.Metadata; metadata != nil && !metadata.ExpiresAt.IsZero() && !time.Now().Before(metadata.ExpiresAt) {
			v, err = b.sessionLog.Apply(c.info.clientID, v, sessionstore.Batch{Remove: 1, ExpectedEpoch: &c.durableEpoch})
			if err != nil {
				return rollback, err
			}
			continue
		}
		if remaining <= 0 {
			break
		}
		var id uint16
		for n := 1; n <= 65535; n++ {
			if !used[uint16(n)] {
				id = uint16(n)
				break
			}
		}
		if id == 0 {
			break
		}
		used[id] = true
		remaining--
		v, err = b.sessionLog.Apply(c.info.clientID, v, sessionstore.Batch{Move: []uint16{id}, ExpectedEpoch: &c.durableEpoch})
		if err != nil {
			return rollback, err
		}
	}
	return rollback, nil
}

const defaultDurableWindow = 128
const maxDurableWindow = 65535

func (b *Broker) durableWindow() int {
	if b.sessionWindow <= 0 {
		return defaultDurableWindow
	}
	if b.sessionWindow > maxDurableWindow {
		return maxDurableWindow
	}
	return b.sessionWindow
}

// Caller holds the client-ID lock, before outboundSendMu. All new durable
// deliveries join the FIFO, including online/retained deliveries.
func (c *client) enqueueSessionDelivery(packet *packets.PublishPacket) error {
	c.outboundSendMu.Lock()
	defer c.outboundSendMu.Unlock()
	epoch := c.durableEpoch
	err := c.broker.applySession(c.info.clientID, sessionstore.Batch{ExpectedEpoch: &epoch, Enqueue: []sessionstore.Message{{Topic: packet.TopicName, Payload: packet.Payload, QoS: packet.Qos, Retain: packet.Retain, Metadata: packets.CloneMetadata(packet.Metadata)}}})
	if err != nil {
		return err
	}
	return c.pumpSessionQueueLocked()
}

func (c *client) pumpSessionQueue() error {
	c.outboundSendMu.Lock()
	defer c.outboundSendMu.Unlock()
	return c.pumpSessionQueueLocked()
}

// Lock order: client-ID -> outboundSendMu -> stateMu -> inflightMu -> store.
// Move commits before registration or wire output; ACK deletion commits before
// this function can reuse the identifier. Never remove/recreate a session here.
func (c *client) pumpSessionQueueLocked() error {
	if !c.durableReady {
		return nil
	}
	for {
		c.stateMu.Lock()
		if c.status == Disconnected {
			c.stateMu.Unlock()
			return nil
		}
		c.inflightMu.Lock()
		messages, version, err := c.broker.sessionLog.ReadQueue(c.info.clientID, 0, 1, int(^uint(0)>>1))
		if err != nil || len(messages) == 0 {
			c.inflightMu.Unlock()
			c.stateMu.Unlock()
			return err
		}
		message := messages[0]
		// Expiry applies only before the delivery enters the QoS exchange.
		if metadata := message.Metadata; metadata != nil && !metadata.ExpiresAt.IsZero() && !time.Now().Before(metadata.ExpiresAt) {
			_, err = c.broker.sessionLog.Apply(c.info.clientID, version, sessionstore.Batch{ExpectedEpoch: &c.durableEpoch, Remove: 1})
			c.inflightMu.Unlock()
			c.stateMu.Unlock()
			if err != nil {
				return err
			}
			continue
		}
		if len(c.inflight) >= c.broker.durableWindow() {
			c.inflightMu.Unlock()
			c.stateMu.Unlock()
			return nil
		}
		var id uint16
		for n := 1; n <= maxDurableWindow; n++ {
			if _, used := c.inflight[uint16(n)]; !used {
				id = uint16(n)
				break
			}
		}
		if id == 0 {
			c.inflightMu.Unlock()
			c.stateMu.Unlock()
			return nil
		}
		_, err = c.broker.sessionLog.Apply(c.info.clientID, version, sessionstore.Batch{ExpectedEpoch: &c.durableEpoch, Move: []uint16{id}})
		if err != nil {
			c.inflightMu.Unlock()
			c.stateMu.Unlock()
			return err
		}
		packet := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		packet.TopicName, packet.Payload, packet.Qos, packet.MessageID, packet.Retain = message.Topic, message.Payload, message.QoS, id, message.Retain
		packet.Metadata = packets.CloneMetadata(message.Metadata)
		valid := c.session.UpdateOutbound(c.outboundOwner, id, &sessions.OutboundMessage{Packet: packet})
		if valid {
			c.inflight[id] = &inflightElem{status: Publish, packet: packet, timestamp: time.Now().Unix()}
		}
		c.inflightMu.Unlock()
		c.stateMu.Unlock()
		if !valid {
			return sessionstore.ErrConflict
		}
		if c.broker.durableRouteHook != nil {
			c.broker.durableRouteHook(c.info.clientID, "persisted")
		}
		if err := c.WriterPacket(packet); err != nil {
			return errors.Join(err, errDurableAccepted)
		}
		c.ensureRetryTimer()
	}
}
