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
- Inbound QoS2 PUBLISH from client connections is rejected by disconnecting,
  without PUBREC, whenever this option is enabled. Inbound PUBREL state is not
  persisted. This is not a complete MQTT persistent-session implementation.
- Fanout is not atomic with publisher ACK, retained updates, or other sessions. No end-to-end
  exactly-once or crash-atomic fanout guarantee is claimed.
- Enqueue, new outbound inflight, ACK, epoch advancement and queue movement use
  incremental Apply records, not backlog snapshots. Individual records must fit
  the log's maximum record size. Queued deliveries need not fit the available
  packet IDs simultaneously. There is no quota,
  compaction, expiry, cross-node replication, or cluster ownership protocol.
- A failed CONNACK rollback can itself fail if storage becomes unavailable or
  the saved snapshot exceeds the maximum record size. That
  failure is logged; the poisoned store must be reopened. No success is sent on
  a pre-CONNACK storage failure. This is not a crash-atomic handshake protocol.

Verification: `go test ./broker -run TestSessionLogMQTT -count=1` exercises MQTT
SUBACK, offline publish/PUBACK across broker reopen, recovered QoS1 acknowledgment,
QoS2 restart before PUBREC, restart after PUBREL, and durable PUBCOMP deletion.
This is a reopen integration test, not a SIGKILL or power-failure test.

`go test ./broker -run TestSessionLogWindowDrainsFIFO -count=1` covers QoS1/2
with a two-entry window, a larger queued backlog, online tail admission, ID
reuse, mid-drain restart, PUBREC slot retention, and complete durable draining.
