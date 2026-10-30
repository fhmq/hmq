# Event store

This is a single-node store used by the broker's experimental reliable cluster.
It does not implement replication or consensus.
Each event ID, payload, and complete sorted target set is committed in one storage
WAL record. Confirmations are separate durable records. Retrying an identical
event or confirmation is idempotent; conflicting reuse of an ID is rejected.
Event IDs and target identifiers must be nonempty valid UTF-8 strings.
Completed events remain retained for deduplication, so configured capacity is a
hard lifetime limit, not a pending-queue limit. Limits cover retained event count
and ID, payload, and target bytes, not WAL size or total memory. There is no
compaction or deletion. Reopening with limits below recovered usage fails.

`Pending(target, limit)` returns up to a positive limit of events awaiting that
target, ordered by their first successful insertion. It returns a snapshot with
independent copies; acknowledgements and inserts after the call can change a
later result. This order is preserved on recovery. Empty or invalid UTF-8 targets
and nonpositive limits are rejected.

`PendingAfter` pages after an insertion-sequence cursor, including when that
cursor's event was acknowledged. `Sequence` and `PendingUntil` allow a scan
to use a fixed inclusive upper bound while new events are inserted. Per-target
indexes use O(pending target memberships) memory; locating a page takes
O(log n), followed by O(page size) traversal, excluding event copying. Indexes
are reconstructed from the WAL on reopening.

The following measures full-backlog pagination, not a single page or disk I/O:

```sh
go test ./broker/eventstore -run '^$' -bench '^BenchmarkPendingAfterBacklog$' -benchmem
```

On an Intel i5-8279U macOS host, a short 100 ms benchmark run measured about
0.64 ms for 4,096 events, 14.5 ms for 65,536, and 112 ms for 262,144.
These are diagnostic observations, not performance guarantees or regression
thresholds; copying results still allocates memory proportional to the scan.

An append error has an uncertain outcome. Close and reopen before retrying;
recovery may include the record. Storage rejects incomplete or corrupt tails
instead of truncating them, so a crash with a partial write can require operator
recovery. Durability and file ownership inherit the storage package limitations.
This does not make delivery or external business side effects exactly once:
consumers must handle redelivery and coordinate their own side effects.
