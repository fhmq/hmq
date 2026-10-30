# Embedded replicated partition

This package is an experimental single-partition kernel using HashiCorp Raft.
The broker's opt-in `cluster.quorum` mode connects it to a dedicated mTLS
transport and a restricted MQTT publication path. No external coordination
service is required. Default, best-effort and reliable cluster modes retain
their previous acknowledgment semantics; enabling this package alone changes
no MQTT behavior. See [cluster configuration](../../conf/cluster.md) for the
quorum mode's supported operations and explicit exclusions.

Open requires an existing dedicated data directory and a caller-owned Raft
transport. Bootstrap configuration is used only when durable state is empty.
The directory is bound to the configured node ID and cluster ID. Broker callers
must supply ClusterID; empty ClusterID is reserved for standalone kernel tests.
Every member must use the same initial voting configuration. The caller owns the
transport and may close it concurrently with Node.Close to abort stalled RPCs
during shutdown. The TLSStream adapter enforces fixed network identities.
Address discovery, follower forwarding, membership changes and partition
placement are not implemented. In-memory transports in kernel tests are not
production transport.

Publish succeeds only after quorum commitment and local state-machine apply.
Followers reject leader-only proposals. A timeout or lost leadership has an
uncertain outcome: retry the same ID and payload. IDs are at most 256 UTF-8 bytes,
payloads at most 1 MiB. At most 128 proposals are outstanding per node, including
timed-out requests whose Raft futures have not resolved. Further requests get
ErrBusy. Entries is a local applied view, not a linearizable read; a follower
may lag. Duplicate IDs with identical payloads return their original index;
conflicting reuse is rejected by the replicated state machine.
Raft append batches are capped at four entries to leave room for nested WAL
encoding within the storage record limit; smaller caller settings are kept.

The state machine currently retains all publications for deduplication. It has
deterministic limits of 100,000 accepted entries and 64 MiB of IDs plus payloads,
not bounds on WAL size or total process memory. Capacity rejection itself may
occupy a Raft log entry. Snapshots retain publication data and deduplication;
there is no consumer retention watermark or safe event deletion yet. Nodes must
run matching code and limits. Existing storage corruption and filesystem sync
limitations apply; this does not add hardware failure or power-loss guarantees.

This kernel is not Kafka: it lacks partition placement, consumer offsets,
replicated MQTT sessions and an operational upgrade protocol.

## Authenticated streaming transport

ListenTLS supplies Raft's StreamLayer using a dedicated TCP listener and the
cluster's mTLS certificate identity policy. Each leaf certificate has exactly
one URI SAN `urn:hmq:node:<ID>`. Outgoing connections also verify the expected
member's ID and DNS/IP name; incoming connections must belong to the fixed map.
Self-connections are rejected. Both sides exchange a bounded, TLS-protected
`hmq-raft/1` preface containing cluster ID and the sorted ID/address map digest.
It is intentionally a different protocol from best-effort cluster frames.

Dialing, TLS and preface exchange share one deadline. Handshake concurrency is
limited to 32 and total tracked connections to 128. Close cancels dialing and
aborts sockets, including streams already handed to Raft. The caller must not
mutate certificates, private keys or CA pools after constructing the layer.
An injected Listener transfers ownership only on successful construction.

These checks isolate correctly configured, cooperating members. Raft assumes
crash faults, not Byzantine peers: this layer does not verify that every Raft
RPC body claims the same identity as its connection certificate.
