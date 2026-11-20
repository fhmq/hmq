# Local container validation

## QoS 2 session persistence

Date: 2026-11-20 (Asia/Shanghai). Source base: `142afdc` plus the QoS 2
session persistence changes. The same Colima profile below ran an independent
Compose project, `hmq-qos2-final`, without replacing the original deployment.
Tested image: `sha256:8483f788e0623b13f078aa306baea47408e79f31672029ef2652332df746fa16`.

With `HMQ_COMPOSE_TEST=1`, the complete race-enabled Compose suite passed in
33.833 seconds. It covered standalone SIGKILL and restart, publisher recovery
after leader change c -> a, subscriber PUBREL recovery on c after a full
cluster restart, and no PUBCOMP with only one member online. MQTT 5 resumed
on the same broker and across a/b, preserving the message content type and
subscriber Packet ID. Duplicate PUBLISH and PUBREL produced one delivery.
The full repository race suite, go vet and git diff --check passed.

The test deployment remains on loopback MQTT ports 11983 / 21983 / 31983 /
41983 and HTTP ports 18180 / 28180 / 38180 / 48180 for single / a / b / c.

## Original Compose validation

Date: 2026-11-19 (Asia/Shanghai).

## Environment

- Source base: `ed0126c`, plus this branch's Compose/configuration changes.
- Independent Colima profile: `hmq-test`, Linux amd64, 4 CPUs, 4 GiB RAM, 20 GiB disk.
- Docker client 29.10.0, engine 29.2.1, Compose 5.8.0.
- Broker compiled inside the Go 1.27.4 builder; not a prebuilt release binary.
- Tested image: `sha256:3f72ea064fe954fa04353f55d6103d6bea8541481ca51b0b96d0f3d073625399`.

## Results

The final real-container suite used `HMQ_COMPOSE_TEST=1` and
`go test -race -v -count=1 -timeout=10m ./deploy/compose/e2e`.
It passed in 16.030 seconds, including these observed results:

| Check | Result |
| --- | --- |
| Standalone QoS0/1/2, wildcard subscriptions and retained replay | PASS |
| MQTT 5 properties and MQTT 3/5 interoperability | PASS |
| MQTT 5 Will after abrupt client close | PASS |
| Follower-originated cross-node publication, including MQTT 5 metadata | PASS |
| Stop leader `b`, elect leader `c`, publish across surviving nodes | PASS |
| Stop second node; new QoS1 publication gets no success acknowledgment | PASS; client reports connection lost before publication completed |
| Restore all nodes and resume delivery | PASS |
| Stop/start entire cluster with data volumes retained | PASS |
| Recover delivery cursors | Before: a=17, b=17, c=19; recovered: a=b=c=19 |
| New publication after full restart | PASS |

Final HTTP diagnostics showed a/b Follower, c Leader, cursor=21 on every node,
`consumerFailed=false`, and no active proposal requests. All four broker services
were left running, with only loopback host port bindings.

The ordinary full repository race regression also passed: 21 packages, no failing
packages. Compose tests skip unless explicitly enabled; the container run above
was separate and actually exercised Docker. Static analysis and Compose config
validation passed as well.

A subsequent pre-release repeat passed in 26.289 seconds: leader c failed over
to b, the minority did not acknowledge a publication, and cursors a=27/b=29/c=27
recovered to 29 on every node after the full restart. Leader identity and cursor
values naturally change between repeated test runs.

## Configuration bug found by deployment

The first cluster launch failed because JSON `plugins.bridge: "none"` created a
non-nil mock bridge, which quorum correctly rejected as an enabled bridge.
Configuration parsing now leaves the bridge disabled for `none` or the empty name,
while preserving authentication setup and the existing bridge factory API.
`TestDisabledBridgeJSONDoesNotEnablePlugin` covers this regression. The final image
and container test run include that fix.

## Use the currently running local environment

On this machine the test tools are installed privately, without switching the
default Docker context. From the repository root:

```sh
export PATH="$HOME/.local/hmq-container-tools/docker:$HOME/.local/hmq-container-tools:$PATH"
export DOCKER_HOST="unix://$HOME/.colima/hmq-test/docker.sock"
docker-compose -f deploy/compose/compose.yaml --profile single --profile cluster ps
HMQ_COMPOSE_TEST=1 go test -race -v -count=1 -timeout=10m ./deploy/compose/e2e
```

For a standard Docker Desktop installation use the general README commands.
Do not expose this mock-auth local environment publicly. These results do not
constitute multi-host, production-load, packet-loss/latency, or power-failure tests.
