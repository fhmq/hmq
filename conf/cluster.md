# Direct cluster configuration

This section describes the default best-effort direct mode. The optional
[quorum mode](#experimental-quorum-configuration) below has different guarantees
and deliberately narrower MQTT support.

The direct cluster implementation is experimental. Configuring `cluster`
starts direct broker-to-broker connections without the legacy router. Connections
require mutual TLS and a shared secret.

Each node must be directly connected to every other node. Configure seeds so
that every pair of nodes has a connection; seeds are not a discovery service.
Messages are forwarded once to connected peers, without persistence or delivery
acknowledgements. Disconnects, restarts, and network partitions can lose messages;
reconnecting does not replay them or synchronize retained state. Cross-node QoS
and shared subscriptions do not have Kafka-like durability or coordination.
Retained messages are forwarded only to peers connected at publication time;
this is not replicated state. During a partition, nodes can retain different
values (including a value on one node and a deletion on another) for the same
topic. Reconnection performs no conflict resolution, so MQTT subscribers can
receive different retained values depending on which node they connect to. A
later retained publication is forwarded to currently connected peers, but is
not guaranteed to repair every node if delivery fails.
Nodes exchange heartbeats to detect silent connection failures. A full inbound
message queue closes the affected peer connection and triggers reconnection;
queued or in-flight messages can be lost. The current direct-cluster protocol
is version 3 and cannot communicate with earlier experimental versions. Upgrade
all nodes together rather than mixing protocol versions.

Example `cluster` object (use a unique nodeID per node and replace the secret
with a shared, confidential value):

```json
{
  "nodeID": "node-1",
  "clusterID": "example-cluster",
  "secret": "replace-with-a-private-shared-secret",
  "listen": "127.0.0.1:1993",
  "seeds": ["127.0.0.1:1994"],
  "tls": {
    "certFile": "cluster/node-cert.pem",
    "keyFile": "cluster/node-key.pem",
    "caFile": "cluster/ca.pem"
  }
}
```

Put this object under the top-level `cluster` key. All fields except `seeds`
are required when configuring a cluster, including all three `tls` files.
Each node needs a certificate and private key suitable for both client and
server authentication, signed by a CA trusted by its peers. Certificate names
must match the addresses used to reach the node. Each node also needs a distinct
certificate with exactly one URI SAN, `urn:hmq:node:<nodeID>`; see
[`IDENTITY.md`](../broker/cluster/IDENTITY.md) for the identity requirements.
The CA file establishes peer
trust. These files are separate from the broker's client-facing `tlsInfo`.
Paths are resolved relative to the process working directory. Validation checks
that the key pair matches and the CA contains a parseable certificate; it does
not verify certificate validity, usages, names, or trust chains. Addresses use host:port syntax; IPv6
addresses require brackets. The top-level `router` field, router flags, and
legacy cluster `host`/`port` fields are rejected. Protect configuration files
from unauthorized access because they contain the shared secret.

## Experimental reliable configuration

The experimental `cluster.reliable` configuration has `enabled`,
`memberIDs`, `eventLogPath`, and `inboxLogPath`. When enabled, `memberIDs`
must list the local node and at least one peer, with unique valid node IDs.
The event, inbox, and optional retained logs must be distinct paths. All nodes
must be configured with the same fixed membership. Paths are relative to the
process working directory.

Cluster handshake protocol v3 authenticates the reliable mode and a SHA-256
digest of the sorted member IDs. Nodes with different membership, or with
reliable mode enabled on only one side, cannot connect. Peer IDs must also
belong to that membership. Version 3 is incompatible with older cluster
handshakes, including in best-effort mode; upgrade all members together.

This mode is opt-in. Every member must enable it; it does not interoperate
with best-effort publication frames. The two logs are bound to the cluster ID,
node ID, canonical paths, fixed member set and a shared random identity. Keep
the pair together; changed membership or paths, a missing log, or mismatched
identities fail startup. A partial initialization can require operator recovery.

An acknowledgment means that the receiving node persisted the message in its
local inbox, not that subscribers durably received or acknowledged it. The MQTT
publisher's PUBACK or PUBREC only confirms local acceptance; it does not mean
that every configured cluster member has acknowledged the message. Inbox
processing may repeat after a crash. Delivery is at least once at the processing
boundary, with no ordering guarantee, consensus, or replicated storage. A log
append failure can have an uncertain outcome and requires reopening the logs;
completed identities are retained and the logs have no compaction. Back up and
monitor local storage, and do not delete or replace logs while the broker runs.
Each peer processes pending events in batches of at most 256. Batches rotate
even without acknowledgments, allowing later events to be attempted. Events
that remain unacknowledged stay in the local log and are retried on subsequent
rotations.

Without `experimentalSessionLogPath`, MQTT session state is held in memory.
A non-clean session can recover
its subscriptions and previously in-flight outbound messages after a client
reconnects to the same running broker, but publications made while that client
is offline are not queued for it. Broker restart loses sessions, including
subscriptions and in-flight state. Reliable cluster inbox persistence does not
change these session guarantees.

`experimentalSessionLogPath` is an experimental, optional local log path. When
set, the broker opens and replays the session log at startup and
holds its exclusive lock until shutdown; invalid or locked logs fail startup.
It enables experimental single-node persistence of ordinary non-clean
subscriptions, offline QoS 1/2 queues, and outbound QoS protocol state.
The outbound window defaults to 128 messages; further queued messages advance
after PUBACK or PUBCOMP is durably recorded. Persistent shared subscriptions
are rejected. Inbound QoS 2 PUBLISH is rejected by disconnecting whenever this
option is enabled, so this is not a complete MQTT persistent-session mode.
It provides no cross-node session ownership, replicated session storage, or
crash-atomic fanout. CONNECT rollback still copies the session snapshot; overall
recovery memory is not bounded independently of backlog. See
[`RUNTIME.md`](../broker/sessionstore/RUNTIME.md) for the operational limits.
The path must differ from retained and reliable log paths, including aliases
through symlinks or hard links. Leave it empty unless explicitly evaluating
these experimental guarantees and restrictions.

The monitor endpoint `GET /api/v1/cluster/reliable` reports whether this mode
is disabled, running, degraded, or closed, and counts links with permanent
recovery errors and inbox consumers whose most recent attempt failed. It does
not expose error details or guarantee that a peer is connected or caught up;
use the cluster peer status and storage monitoring as well. A consumer's error
count can clear after a later successful poll.

## Experimental quorum configuration

Quorum is opt-in and mutually exclusive with `cluster.reliable.enabled`. All
members must use the same fixed `clusterID` and peer/address map. Each node has
a distinct `nodeID`, mTLS certificate with `urn:hmq:node:<nodeID>`, private data
directory and dedicated Raft TCP listen address. TLS is at least 1.2; peer identity,
cluster identity and the fixed address-map digest are authenticated in both
directions. The local ID/address MUST appear in `peers` and match `quorum.listen`.

Example addition to the existing `cluster` object on node `a`:

```json
"quorum": {
  "enabled": true,
  "dataDir": "data/quorum-a",
  "listen": "127.0.0.1:2993",
  "peers": {
    "a": "127.0.0.1:2993",
    "b": "127.0.0.1:2994",
    "c": "127.0.0.1:2995"
  }
}
```

Keep the existing cluster node/cluster identity, secret, listen and TLS settings.
The Raft port is separate from `cluster.listen` and the MQTT port. Data directories
are created with mode 0700 and cannot overlap other storage paths, including
symlink aliases. The Raft WAL and delivery cursor bind node ID and cluster ID;
reusing the directory under another identity fails startup. Do not change fixed
membership/addresses or selectively delete/restore files in an existing directory.
Both `raft.wal` and `delivery.wal` must exist together (or both be absent for a
fresh node). Partial initialization/deletion fails closed and requires operator
recovery of complete node state. A cursor beyond the local log/snapshot upper
bound also fails startup; that upper bound is not used as a commit proof.
Dynamic membership and migration are not implemented. Leave quorum settings absent
when disabled; the default routing behavior does not change.

This is one fixed experimental Raft partition, not topic partitioning or replicated
MQTT sessions. Leaders and followers accept ordinary non-retained QoS 0/1 publishes.
Followers forward to the observed leader over authenticated cluster connections.
Configure every other member's **cluster** listen address in each node's `seeds`;
Raft peer addresses are separate and cannot supply these direct connections.
Missing connections, leadership loss and expired deadlines never produce PUBACK.
Each broker admits up to 128 concurrent proposal requests,
rejects excess attempts as busy without queuing, and sends PUBACK only after
majority commit/local FSM application. Loss of the majority cannot produce a
successful acknowledgment. Timeouts/lost leadership/lost PUBACK are uncertain
outcomes: the entry might later commit. Internal forwarding retries preserve the
operation ID and payload and are FSM-deduplicated, including across leaders.
MQTT reconnect/republication creates a new operation and may duplicate it. QoS 0 has no
positive acknowledgment, but still must commit before it is eligible for delivery.

Every member independently consumes committed entries in order and routes them
only to local online subscribers, without forwarding through the old cluster
channel. After successful local routing it fsyncs a node/cluster-bound cursor in
`dataDir/delivery.wal`. Failed delivery retries without advancing the cursor;
failed WAL writes stop consumption until restart. Crash recovery can repeat an
entry, including partial fanout. This is NOT exactly-once or durable subscriber
receipt: PUBACK confirms the publication log, not every replica's delivery or a
subscriber's acknowledgment. No-subscriber entries are considered locally routed.
The log and cursor have no compaction; monitor disk use and kernel capacity.

Only clean online sessions are supported. Reconnect and resubscribe after restart;
subscriptions and outbound inflight state are not replicated or persisted. QoS 2,
retained publications/storage, shared subscriptions, QoS 2 subscriptions, will,
non-clean sessions, bridge plugins and `experimentalSessionLogPath` are rejected.
Reserved `$` publication topics, directed delivery and legacy cluster messages
cannot bypass the quorum path. No bridge side effect executes before commit.
These restrictions are explicit extension points, not complete MQTT durability.

`GET /api/v1/cluster/quorum` reports local Raft `state`, observed `leaderID`,
`partitions: 1`, `routing: "quorum_qos01_nonretained"`, the durable delivery
`cursor` and `consumerFailed`. It is a local diagnostic, not a linearizable read
or proof of current quorum connectivity. Consumer errors do not reveal paths or
payloads in this endpoint. See [runtime notes](../broker/QUORUM.md).
`activeRequests` and `proposalLimit` describe broker request admission; they do
not count timed-out Raft futures that may still finish in the kernel.
