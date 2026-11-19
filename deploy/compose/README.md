# Local Docker Compose deployment

This environment runs the current source build, not a downloaded release image.
It is for local functional and fault-recovery evaluation, not production.
Use a Git checkout or GitHub's source archive for the desired tag. The prebuilt
binary archives include these instructions, but not the complete Go builder source.

## Start

From the repository root, with Docker Engine and Compose available:

```sh
docker compose -f deploy/compose/compose.yaml --profile single --profile cluster up -d --build
docker compose -f deploy/compose/compose.yaml --profile single --profile cluster ps
```

Use only `--profile single` for standalone, or `--profile cluster` for the three
quorum nodes. Run both profiles for the complete test suite below.

| Service | MQTT endpoint | Diagnostic HTTP |
| --- | --- | --- |
| single | 127.0.0.1:11883 | 127.0.0.1:18080 |
| a | 127.0.0.1:21883 | 127.0.0.1:28080 |
| b | 127.0.0.1:31883 | 127.0.0.1:38080 |
| c | 127.0.0.1:41883 | 127.0.0.1:48080 |

All published ports bind to loopback. Cluster and Raft ports are internal only.
Each node has its own data volume. The init service generates a local CA, distinct
node certificates with URI SANs, and a random shared secret inside a named volume.
The CA private key is not saved. Certificates and configurations are preserved on
subsequent starts; private files are readable by the broker's UID 65532.

```sh
curl http://127.0.0.1:28080/api/v1/cluster/quorum
curl http://127.0.0.1:28080/api/v1/cluster
```

## Functional and cluster tests

```sh
HMQ_COMPOSE_TEST=1 go test -v -count=1 -timeout=10m ./deploy/compose/e2e
```

Without the environment variable these tests skip, so ordinary `go test ./...`
does not stop containers or require Docker. The harness uses `docker-compose` if
available, otherwise `docker compose`. Set `COMPOSE_BIN` to an absolute standalone
Compose executable if needed. Docker context / `DOCKER_HOST` are inherited.

Tests create real host-to-container MQTT connections and exercise:

- Standalone MQTT 3.1.1 QoS0/1/2, wildcard subscriptions and retained replay;
  MQTT 5 Will publication after an abrupt connection close.
- MQTT 5 QoS1, user/content-type property forwarding and mixed-version delivery,
  both standalone and across cluster nodes.
- Cross-node quorum publication through a follower.
- Stopping the leader, re-election, and cross-node delivery afterward.
- Stopping a second node: the remaining node must not positively acknowledge a
  new QoS1 publication. A timeout is an uncertain outcome, not proof of rollback.
- Starting all nodes, then stopping and restarting the complete cluster while
  preserving data volumes; delivery cursor recovery and new publication afterward.

The cluster test deliberately stops and starts services `a`, `b`, and `c` in this
Compose project. Do not run it against an environment serving real users or run
multiple copies concurrently. It restores the nodes on completion. MQTT sessions
are clean and resubscribed after reconnect; this does not test replicated sessions.

## Stop and reset

```sh
docker compose -f deploy/compose/compose.yaml --profile single --profile cluster down
```

This preserves credentials and durable logs. To deliberately erase **all** local
test credentials and broker state, add `--volumes` to that command. Never remove
only the credential volume while preserving cluster data, or copy one node's
WAL/cursor to another node. Expired certificates require a coordinated local reset.

## Limits

Client MQTT and diagnostics are unauthenticated local evaluation endpoints using
mock authentication. Only internal cluster traffic uses mTLS. The shared read-only
credential volume is convenient for local evaluation, not production key isolation.
Do not expose these ports externally or reuse these certificates/secrets elsewhere.
Quorum is experimental, single-partition, clean-session QoS0/1 only; retained,
QoS2, shared subscriptions and wills are not available in quorum mode. See
[cluster operations](../../conf/cluster.md). This is not multi-host, long-duration
load, network-latency, or power-loss qualification.

See [local validation record](VALIDATION.md) for the actual run and this machine's
isolated Colima context/tool paths.
