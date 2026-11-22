# Experimental broker integration

Setting `experimentalSessionLogPath` now enables real, single-node runtime
integration for non-clean ordinary MQTT sessions. With the option absent, the
existing memory-only behavior is unchanged.

Implemented paths:

- Ordinary filters are saved before SUBACK and removed before UNSUBACK.
- Publications at QoS 1/2 can be queued for disconnected sessions, including
  sessions discovered from the log after broker restart. Effective QoS 0 is not
  queued. Existing MQTT topic matching rules are reused.
- Reconnect restores filters and converts only a bounded queued prefix into
  durable inflight entries before sending. Recovered PUBLISH uses DUP and the saved
  packet ID; recovered QoS2 PUBREL retains its packet ID.
- Online outbound QoS1/2 is saved before writing PUBLISH. PUBACK/PUBCOMP deletion
  and PUBREC phase advancement are saved before the corresponding runtime
  transition or PUBREL write. Experimental ACK handling is fenced by the
  per-client connection lock. Recovery sends entries in saved sequence order.
- For persistent sessions, inbound QoS2 stores the original PUBLISH before
  PUBREC. PUBREL completes the exchange and enqueues matching durable deliveries
  in one WAL record before PUBCOMP. Replaying PUBREL does not enqueue again.
- Clean CONNECT deletion, persistent session creation/epoch advancement and
  queue-to-inflight conversion commit before successful CONNACK. A failed
  CONNACK write rolls back durable state while preserving the previous owner.
- A dedicated, independent MQTT topic index matches persistent filters once per
  publication. Each matching client is revalidated under its client-ID lock,
  where online durable inflight or offline enqueue is selected. A disconnect
  before inflight registration falls back to enqueue; after registration the
  persisted inflight survives. Unmatched publications do not write the WAL.
- Retained replay takes the client-ID lock before outbound locks. Every
  connection-owned Apply carries the persistent epoch assigned at CONNECT;
  stale epochs cannot update a new session, even if packet IDs are reused.
- The outbound window defaults to 128 inflight deliveries, with an absolute
  maximum of 65,535 MQTT packet identifiers. The internal broker test override
  is clamped to that maximum; no global configuration field is introduced.
  Backlog size is independent of the window. ACK deletion commits before an ID
  is reused to Move the next FIFO queue entry. QoS2 PUBREC does not release a
  slot; PUBCOMP does. New online and retained QoS1/2 deliveries also enter this
  FIFO, so they cannot bypass an older queued suffix. Initial replay finishes
  before pumping newer entries. Existing legacy inflight entries are retained
  and retransmitted even when their count exceeds the default window; no new
  entries are admitted until that count falls below the window.
- Subscription/inflight loading uses paginated reads; replay order comes from
  ReadInflight sequence order, without a queue snapshot for sorting. However,
  prepareSessionLog still takes a complete session snapshot for failed-CONNACK
  rollback (and lifecycle checks can read snapshots). Overall reconnect memory
  is therefore not bounded by the outbound window. The store also holds the
  complete backlog in memory; this is not a disk-only streaming queue.

Explicit exclusions and remaining limitations:

- Persistent shared subscriptions are rejected with SUBACK failure. Existing
  logs containing shared filters cannot be resumed. Clean shared subscriptions
  retain their previous memory-only behavior.
- The atomic fanout applies to durable subscriber queues on inbound QoS2, not
  to QoS1, transient subscribers, retained updates, or external side effects.
  Durable inbound QoS2 does not support bridge plugins. No end-to-end business
  exactly-once guarantee is claimed.
- Enqueue, new outbound inflight, ACK, epoch advancement and queue movement use
  incremental Apply records, not backlog snapshots. Individual records must fit
  the log's maximum record size. Queued deliveries need not fit the available
  packet IDs simultaneously. There is no compaction, session expiry timer,
  cross-node replication, or cluster ownership protocol in the standalone log.
- A failed CONNACK rollback can itself fail if storage becomes unavailable or
  the saved snapshot exceeds the maximum record size. That
  failure is logged; the poisoned store must be reopened. No success is sent on
  a pre-CONNACK storage failure. This is not a crash-atomic handshake protocol.

Verification: `go test ./broker -run TestSessionLogMQTT -count=1` exercises MQTT
SUBACK, offline publish/PUBACK across broker reopen, recovered QoS1 acknowledgment,
QoS2 restart before PUBREC, restart after PUBREL, and durable PUBCOMP deletion.
This is a reopen integration test, not a SIGKILL or power-failure test.

The broker depends on `broker.SessionStorage`, not the WAL implementation.
`broker.NewBrokerWithSessionStorage` accepts an alternative standalone backend.
Implementations must provide durable CAS versions (including deleted sessions),
ownership epochs, ordered queue/inflight reads, idempotent inbound acceptance,
and an atomic release-plus-fanout. The built-in quorum option is a separate
replicated backend; a local database cannot replace its consensus log.

## Pending-message quota and capacity

Both built-in backends support the optional top-level configuration:

```json
"sessionQuota": {
  "maxPendingMessages": 1000,
  "maxPendingBytes": 16777216
}
```

Each dimension is per stored session; zero disables that dimension. The
quota counts queued deliveries, outbound inflight deliveries and incoming
QoS 2 PUBLISH awaiting PUBREL. Bytes charge topic, payload and serialized
MQTT 5 metadata, not on-disk record overhead or process memory. Moving an
entry to inflight does not free capacity. ACK, expiry before delivery or
session deletion can free it. A lower limit preserves previously accepted
messages and allows them to drain.

When enabled, quota admission is all-or-nothing across matching durable
queues. MQTT 3.x disconnects without a successful ACK when quota is full;
MQTT 5 rejects PUBLISH with reason `0x97`. QoS 2 checks current targets before
PUBREC, but does not reserve their queue space. If a target fills before
PUBREL, the broker disconnects without PUBCOMP and retains the pending
exchange; clients must use a resumable session to retry PUBREL after room
is available. Retained state, transient subscribers, bridges, and external
side effects are not part of the atomic queue transaction. A quota is not
a global storage cap and does not reclaim journal history.

`GET /api/v1/capacity` on the private monitor listener reports the configured
quota, queue and handshake counts/bytes, oldest known queue age, connected
outbound inflight count, and local process totals for quota rejections and
slow consumer disconnects. Historical entries without enqueue timestamps
are excluded from the age calculation. In quorum mode it also reports the
local Raft history usage and limits; these are not disk usage or a
linearizable cluster snapshot. Avoid summing replicated queue counts across
nodes. The endpoint does not expose client identifiers or message contents.

`go test ./broker -run TestSessionLogWindowDrainsFIFO -count=1` covers QoS1/2
with a two-entry window, a larger queued backlog, online tail admission, ID
reuse, mid-drain restart, PUBREC slot retention, and complete durable draining.
