# Session snapshot store

This package is standalone and is not connected to broker routing or clients.
It stores subscriptions, ordered unsent QoS 1/2 messages without packet
identifiers, and outbound QoS 1/2 inflight state. Put is a compare-and-swap of one complete session snapshot; nil
deletes the session. Get returns the version even for a tombstone. Use that
version for the next Put, retrying conflicts with a fresh snapshot. Callers
must synchronize changes to input objects during Put.

Apply uses the same versioned CAS for an existing session and atomically
removes a queue prefix, updates subscriptions, and appends messages in one
incremental record. Legacy recovery limitations are described below. The total queue can
exceed the record limit, but each batch, including JSON/base64 overhead, must
fit. IDs, filters and topics must be valid UTF-8; payloads are arbitrary bytes.
Callers must also synchronize input objects during Apply.

Batch.Move atomically assigns distinct nonzero packet IDs to the queue prefix
and moves it to outbound inflight, initially in Publish phase. Batch.Acks
applies PUBACK (QoS 1 removal), PUBREC (QoS 2 transition to PubRel), or PUBCOMP
(QoS 2 removal); invalid or out-of-order acknowledgements reject the entire
batch. Each packet ID may occur at most once among ACKs and cannot occur in
both ACKs and Move within a batch. In particular, persist PUBREC before sending
PUBREL, then process PUBCOMP in a separate batch. Allocate IDs and retry duplicate protocol
packets in the caller; these APIs do not generate or send packets. Commit the
transition before sending the corresponding packet. After recovery retransmit
Publish with DUP or PubRel as appropriate. ReadQueue and ReadInflight bound
returned message count and payload bytes, return independent copies and a CAS
version; compare versions across pages and restart on change. A first message
larger than the byte bound returns storage.ErrTooLarge. Inflight pagination is
ordered by its persisted per-session send sequence, not packet ID; pass the
last returned Inflight.Sequence as the exclusive cursor. Put callers must retain
LastSequence and provide unique nonzero sequences for inflight entries. The
counter cannot decrease on replacement and overflow rejects further moves.
Limits exclude topic and descriptor memory; inflight
sorting uses memory proportional to the number of inflight entries.
List enumerates live session IDs in lexical order, with count and ID-byte
bounds; its global sequence changes on every successful write, so restart a
multi-page scan when it changes. ReadSubscriptions pages filters and QoS in
lexical order with count and filter-byte bounds, using the session version.
Both use an exclusive string cursor; an oversized first item returns
storage.ErrTooLarge. These bounds exclude result descriptors and scan CPU.

Each successful Put or Apply is one synced storage log record. A failed append has an
uncertain outcome and poisons the store: close and reopen before any retry.
Recovery rejects invalid records and inherits broker/storage's corruption,
locking, filesystem and durability limitations. A torn append can require
operator recovery; this package never silently truncates it.

New snapshots and batches use formats 3 and 4; formats 1 and 2 remain readable.
Legacy batches reconstruct send order from their Move operations and retain
their original ACK semantics during replay. A legacy snapshot with one
inflight entry is assigned sequence 1; one with multiple inflight entries fails
closed with ErrCorrupt because its original order cannot be reconstructed.
Do not open new-format logs with older implementations.

This is not yet a complete MQTT session implementation: it does not validate
MQTT topic syntax, handle shared subscription policy, store inbound
QoS 2 protocol state, assign packet IDs, deliver messages, or coordinate a
publisher acknowledgement with multiple sessions. There is no multi-session
transaction, queue limit, expiry, compaction, or replication.
Put still rewrites a snapshot and is limited by the record size; use Apply for
large queues. Apply copies queue message descriptors and Get copies the entire
queue and payloads, so memory and CPU costs remain proportional to backlog.
Recovery replays the whole log; tombstones and the log grow without bound.
Before production integration, add queue limits, compaction, remaining protocol
state, and define backpressure.
