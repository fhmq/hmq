# Single-peer outbox

The outbox retains pending messages for one peer in a local append log. It is
not a consensus or replication engine. Capacity limits pending messages and
payload bytes, not disk size or total memory.

`Compact` atomically replaces acknowledged history with the peer identity,
incarnation, next message ID, and pending messages in their original order.
The sequence high-water mark survives even when no messages remain. Compaction
is explicit, blocks queue operations, and holds encoded snapshots in memory
proportional to pending payloads. It is not automatically scheduled.

A replacement error conservatively disables the outbox until close and reopen.
The recovered file determines the installed state. Compaction inherits the
storage package's local filesystem and sync limitations and rejects logs with
multiple hard links. Do not externally alter log paths or create aliases while
the log is open. Version 2 compacted snapshots require the current reader;
original version 1 histories remain readable.

The broker's experimental reliable cluster uses eventstore, not this outbox.
This operation does not reclaim that cluster's event or inbox history, whose
deduplication records still need a separate retention protocol.
