# Experimental single-partition quorum routing

See [configuration and guarantees](../conf/cluster.md#experimental-quorum-configuration).

Leader and followers accept ordinary non-retained QoS 0/1 publications. A bounded
nonblocking admission gate calls `replication.Node.Publish`; up to 128 requests
may run concurrently, and excess attempts return busy immediately rather than
queuing behind a global proposal lock. Each MQTT client's publish lock preserves
its ordering; cross-client ordering is the committed Raft log order.
Raw payloads over the kernel's 1 MiB limit are rejected before JSON encoding;
the complete encoded publication must also fit that limit. JSON/base64 overhead
therefore makes the usable MQTT payload smaller. Request counts do not bound
all decoded network packets, WAL history or total process memory.
PUBACK follows successful
majority commit and local FSM application, not subscriber acknowledgment. No
bridge, retained write or local delivery runs before commit. Followers disconnect
publishers without successful acknowledgment if forwarding cannot commit. QoS 0 also commits before delivery,
but MQTT QoS 0 has no acknowledgment with which to report success to the caller.

Every node polls its own applied `Entries` in log order, delivers locally without
forwarding, then fsyncs the index in `delivery.wal`. The WAL is bound to node and
cluster identity. Delivery errors retry the current entry; poisoned cursor writes
stop the consumer until restart. Monitoring exposes the cursor and failure flag.
The kernel has fixed capacity and no retention/compaction protocol.

Shutdown and initialization rollback close Raft and the network transport
concurrently, interrupting in-flight RPCs instead of waiting for size-scaled
snapshot deadlines. Only Node.Close closes the Raft store, after Raft shutdown.
Admission closes under the same lock that registers in-flight work; shutdown
then waits for those registered requests before closing the delivery cursor.

The cursor marks successful local routing attempts, NOT durable subscriber
receipt. There is no transaction between socket writes and cursor writes. A crash
or partial fanout can repeat delivery to some/all subscribers. There is no MQTT
retry deduplication across connections or lost PUBACKs; retries can append another
entry. A timeout or leadership loss is an uncertain outcome, not proof of abort.

Only clean, online sessions and ordinary QoS 0/1 subscriptions are supported.
Entries consumed with no subscribers are checkpointed. Subscriptions and outbound
inflight state are in memory and are lost on restart; clients must reconnect and
resubscribe. This is durable publication acceptance, not an offline subscriber
queue or exactly-once MQTT implementation. The committed log and local cursor must
be backed up/restored as one node's state, never independently reset or mixed.

QoS 2, retained messages/storage, shared subscriptions, will messages, non-clean
sessions, session logs, bridge plugins, directed delivery, reserved `$` publication
topics and legacy cluster publication frames are rejected. The old direct-cluster
connection carries authenticated quorum forwarding, never legacy publications.
See [forwarding protocol](QUORUM_FORWARDING.md).
Default best-effort and reliable modes are unchanged when quorum is disabled.
