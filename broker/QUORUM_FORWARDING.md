# Follower forwarding

Implemented in `quorum_forward.go`. Followers forward MQTT publications; both
local and remote leaders use `Node.Publish`. The local committed consumer remains
the only delivery path. Existing MQTT topic authentication remains mandatory.

The overall operation budget is five seconds, including retries and SendContext
slot waiting/socket writes. Retries use a 150 ms ticker and request writes use
the full remaining operation context, without a fixed short write budget.
Remaining duration is refreshed immediately before encoding each send.
Only positive responses
are sent; failures are retried against the newly observed leader until the total
deadline. No caller infers success from an error or a missing response.
The wire protocol carries only remaining nanoseconds, never an absolute wall-clock
deadline. A receiver clamps the budget to five seconds and starts its local
monotonic deadline at callback entry; validation, queue waiting, Raft and response
writing all consume it. The origin independently enforces its original deadline,
including network transit, and rejects late success. No synchronized clocks are
required. Nonce generation failure aborts broker initialization with an error.

Limits per broker: 128 admitted calls, 8 MiB pending payload bytes, 8 request
workers plus 8 response workers, 64 slots in each queue, 16 queued/active requests
per authenticated peer and 8 MiB queued/active request wire bytes. Requests must
fit both the 1 MiB final cluster frame and 1 MiB Raft publication limits. These
are runtime budgets, not a bound on all process memory or durable log history.
Responses carry no publication payload. Duplicate waiters consume the same fixed
queue/peer budgets; there is no extra response cache or unbounded goroutine.

Every origin needs direct authenticated connections to every possible leader:
list the other members' cluster listen addresses in `cluster.seeds`, not their
Raft listen addresses. Seeds are not dynamic discovery.

## Identity and completion

The versioned, domain-separated protocol uses authenticated
`cluster.Node.SendContext`, never legacy publication frames. Only fixed quorum
members may send requests or responses. One operation ID per local publish call
combines origin node, random boot nonce and monotonic sequence, unchanged across
attempts and leaders. The receiver validates the origin against the authenticated
peer; this operation is also the Raft ID. MQTT packet IDs are not global IDs.

Publication bytes and digest are frozen before the first attempt. A bounded
pending entry is registered before sending. Responses must match operation,
digest, attempt and actual authenticated target. A valid committed response from
an earlier attempted leader may complete the operation; stale failure responses
cannot supersede later attempts because failure responses are not sent.
Only successful `Node.Publish` with nonzero
index proves commitment. Receiving, queueing, writing a frame or reading
`LastIndex` is not sufficient. Neither forwarding side delivers directly.

FSM deduplication supplies same-ID/same-payload idempotency across internal
retries, leader changes and snapshots; conflicting payload reuse must fail.
It does not make client reconnects or a follower crash exactly once, because
those can create a new operation. No extra permanent response cache is needed.

## Budgets and lifecycle

One overall deadline spans retries. Each attempt reads the observed leader and
only targets a configured member. There is no broadcast, recursive forwarding or
address supplied by a response hint. Timeout or leadership loss is uncertain.

Context-aware sending includes write-slot waiting and socket writes in the
remaining budget, without goroutine wrappers around blocking Send. Final wire
size and Raft payload size include JSON/base64 overhead. Callbacks only validate
and enqueue, or nonblockingly resolve pending calls; they never wait for Raft.

Broker shutdown closes admission and wakes pending calls before waiting for
cluster callbacks. Runtime Close aborts Raft/network work, joins registered
proposals and fixed workers, then closes storage. Registration and shutdown share
a synchronization boundary so WaitGroup Add cannot race Wait.
The current seed list is not discovery: every origin needs a direct connection
to any possible leader, using cluster addresses, not the separate Raft port.

## Coverage and remaining limitations

- Success from a follower, with PUBACK only after quorum commit.
- Lost response retried with one resulting FSM publication.
- Same operation with a conflicting payload rejected.
- Leader changes, shutdown with an uncertain request, and earlier-attempt success.
- Wrong authenticated sender, digest, attempt or operation never resolves a call.
- Saturated counts/bytes fail promptly; no unbounded callback or goroutine work.
- Missing direct connection, oversized frames, and pending-request shutdown.
- Legacy frames still cannot bypass quorum and no delivery runs before commit.

Tests also check relative/clamped budgets, queue time accounting and a request
write longer than 100 ms. They do not exhaustively inject shutdown at every
instruction, real clock skew or late Raft commits after timeout. The protocol has
no wall-clock fields, so correctness does not require synchronized peer clocks.
There is no dynamic discovery, durable follower pending state, offline MQTT
session replication or exactly-once delivery across client reconnects/crashes.
