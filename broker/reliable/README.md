# Reliable peer delivery

`Link` sends to one authenticated peer using `cluster.Node.Send` and a durable
`eventstore.Store`. Enqueue persists a single-target event before scheduling
delivery. Unconfirmed events are retried periodically, including after a send
error or lost ACK. New loads pending events using `Pending(peer, 256)`; retry
cycles rotate through durable pending events in batches independently of ACKs.
Each cycle has a fixed insertion-sequence upper bound, so continuing enqueues
cannot prevent a return to older unconfirmed events.
The in-memory retry batch is bounded to 256 IDs, including runtime enqueues
and resumes. A successful enqueue or resume may remain only in durable storage
until its batch is reached; Pending reports only the in-memory batch. After the
last batch, retries wrap to the beginning, including after restart. An Enqueue
success confirms persistence, not delivery. There is no ordering guarantee for
delivery or completion within a batch.

ACKs are checked against durable records as well as the in-memory retry batch.
Duplicate ACKs for an already confirmed event are idempotent; a valid late ACK
for a record outside the current recovery batch is also persisted. Unknown IDs
and IDs belonging to another peer are rejected.

`New` uses cluster's default 1 MiB maximum frame size. When configuring a
different `cluster.Config.MaxFrameSize`, use `NewWithMaxFrameSize` with the
same value (zero selects the default). `Enqueue` checks the complete encoded
JSON data frame, including escaped ID and base64 payload, before persistence;
it returns `ErrFrameTooLarge` when the limit is exceeded and `ErrFrame` for an
empty or invalid UTF-8 ID. Recovery validates pending records against the same
limit: construction fails on an invalid first batch, and `Resume` returns the
error without scheduling the record. If a later batch contains an invalid
record, background delivery stops and `Err()` reports the error and event ID;
applications must monitor `Err()` and reconstruct the link after correcting the
configuration or stored records. Records are not deleted by this validation.

Use globally unique event IDs, keep the store's pending entries for a peer
exclusive to that link, and route the authenticated peer ID and received frame
to `Handle`. The receiver callback must complete its required durable work
before returning nil. Retries can invoke it multiple times, including after a
successful callback whose ACK was lost; applications must provide their own
durable deduplication if needed. An ACK confirms only what the callback promises,
not exactly-once execution or replicated consensus.

Alternatively, `HandleInbox` delegates durable receipt and deduplication to an
`Inbox`. Its `Accept` operation must atomically persist the payload keyed by
authenticated peer and ID, tolerate identical concurrent retries, and return
`ErrConflict` for a different payload under the same key. Keep these records
for as long as any old frame can be replayed. The implementation must recover
accepted records for application processing after a crash. ACK is sent only
after `Accept` succeeds; a lost ACK causes another identical accept and ACK.
Storage errors and conflicts receive no ACK. The `inbox` subpackage provides a
local durable store and a polling consumer. Create one consumer per peer and
do not run multiple consumers for the same peer: there is no claim mechanism.
The consumer retries failed delivery, reads, and completion after its configured
interval; `Err` exposes the latest attempt error. A successful delivery followed
by a failed completion can be delivered again. `Close` waits for the callback
and completion to return, and does not close the store. Stop producers and
consumers before closing the store. `Stop` requests shutdown without blocking,
including from the delivery callback; `Done` signals when shutdown is complete.
Do not call `Close` from the callback itself.

IDs must never be reused for a peer, including after sender state loss or a new
incarnation. For example, include a freshly generated, collision-resistant
incarnation identifier and a unique per-incarnation message identifier in the
ID, and persist the resulting ID with the outgoing event. An old ACK otherwise
can confirm a different message. Authentication and replay protection are the
caller's responsibility. Inbox persistence is local to its implementation;
this protocol does not provide durable replication. Processing an inbox record
and external side effects are at-least-once unless the application provides
its own transactional or idempotent effects.

`Close` joins the retry worker but not in-flight `Handle` calls; stop and join
those calls before closing the shared sender or store. `Send` must eventually
return for `Close` to finish. The link does not close either dependency. Send
and background store errors are retried on the next tick but are not exposed
through an asynchronous error API. An eventstore append error can require the
store to be closed and reopened; applications need to monitor storage health
and reconstruct the link. There is no cancellation of an in-progress Send,
and no cluster-level protocol multiplexing
provided by this package. Callers must wire callbacks and distinguish these
frames from other protocols. Ensure both peers and the transport use compatible
frame limits; otherwise frames can still be rejected and retried indefinitely.
