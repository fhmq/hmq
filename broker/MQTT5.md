# MQTT 5.0 Profile

The client-facing TCP, TLS and WebSocket listeners detect the CONNECT protocol
version. MQTT 3.x and MQTT 5 clients can publish and subscribe to each other;
no additional listener or configuration switch is required. The wire codec is
Eclipse Paho's MQTT 5 packet library, with broker-side admission and protocol
validation before decoding.

Code embedding the broker should use `github.com/fhmq/hmq/v4/broker/packets`
for the publication API. Its protocol-neutral publish model carries immutable
properties and expiry metadata; MQTT 3 packet wire behavior remains unchanged.

## Supported

- CONNECT/CONNACK, assigned client IDs, username/password authentication and ACLs.
- QoS 0/1/2 in ordinary memory mode, including negative publication acknowledgments.
- SUBSCRIBE/SUBACK, UNSUBSCRIBE/UNSUBACK with per-filter reasons, and PING.
- No Local, Retain As Published and all three Retain Handling values.
- Subscription identifiers. Overlapping filters may produce separate deliveries,
  each containing its matching subscription identifier.
- Incoming topic aliases (up to 32), scoped to the network connection. The broker
  sends full topic names and does not depend on the client's alias capacity.
- User properties, payload format, content type, response topic and correlation
  data on publications, including retained storage and cluster forwarding.
- Message expiry on live, retained and queued deliveries. The remaining lifetime
  is forwarded; an expired message is not newly delivered.
- Will publications, including application properties, payload format and expiry;
  DISCONNECT reason `0x04` requests Will publication.
- Client Receive Maximum for outbound QoS traffic, held until PUBACK/PUBCOMP or
  a negative PUBREC. The broker additionally caps this window at 128 inflight
  publications. Waiting publications are bounded to 256 messages / 4 MiB per
  connection. Queue overflow disconnects the slow consumer without failing an
  unrelated publisher. Shared groups try another eligible consumer on overflow
  or when a member's maximum packet size is too small.
- Client Maximum Packet Size, including CONNACK and control packets. Publications
  exceeding a subscriber's size limit are discarded without consuming its credit.
- Malformed packet checks for bounded variable integers, fixed flags, duplicate
  singleton properties, property contexts, UTF-8, identifiers and topic grammar.

## Negotiated Limits

This is a deliberately bounded profile, not a claim of complete MQTT 5 feature
conformance:

- With a session backend, MQTT 5 supports non-expiring sessions when CONNECT
  requests `SessionExpiryInterval=0xFFFFFFFF`. Clean Start resets the saved
  state; otherwise the broker resumes it and reports Session Present. Finite
  expiry timers are not implemented and finite requests negotiate zero.
  Persistent MQTT 5 subscriptions support basic topic filters and QoS only.
  A Clean Start=false connection that finds an existing active or saved session
  is refused with CONNACK `0x83`, preserving that session instead of silently
  deleting it. Clean Start=false is accepted when there is no existing state.
- A delayed Will cannot outlive a zero-expiry session. Delayed Will scheduling
  for persistent sessions is not implemented. DISCONNECT can request deletion
  of a non-expiring session with a zero expiry override.
- Enhanced AUTH is unavailable. CONNECT requesting an authentication method gets
  CONNACK `0x8C`; username/password authentication remains available.
- The server admits packets up to **1 MiB**, including fixed header, and advertises
  this maximum in MQTT 5 CONNACK. The initial CONNECT is bounded for all versions.
- There are no same-connection timer retransmissions for MQTT 5. Its QoS exchanges
  progress by acknowledgments; MQTT 3 retry behavior is unchanged.
- Quorum always advertises retained and shared subscriptions unavailable. It
  advertises Maximum QoS 1 only when replicated sessions are disabled.
- Existing cluster membership, single-partition replication and crash-fault
  guarantees are unchanged. Expiry timestamps across nodes assume synchronized
  clocks; this implementation is not a clock-skew tolerant expiry protocol.

The existing permissive sample authentication is not suitable for production.

## Connection Admission

All client transports use a CONNECT read/write deadline (including TLS handshake
I/O). Configuration defaults are `connectTimeoutSeconds: 10`,
`maxConnections: 10000`, and `maxConnectionsPerIP: 256`. Zero selects the defaults;
negative values are invalid. Counts include admitted MQTT connections and pending
CONNECTs and are released on disconnect. The deadline is cleared once CONNECT is
parsed so Keep Alive zero remains valid. Limits are local to each broker.

Per-IP accounting uses the socket peer, not forwarded headers. WebSocket clients
behind one reverse proxy therefore share its IP quota. HTTP header reads are also
bounded; use proxy/firewall admission limits for HTTP connections before upgrade.

## Example

With the broker listening on `127.0.0.1:1883`, run these in separate terminals:

```sh
mosquitto_sub -V mqttv5 -h 127.0.0.1 -t demo/mqtt5 -q 1
mosquitto_pub -V mqttv5 -h 127.0.0.1 -t demo/mqtt5 -q 1 -m hello
```

Protocol reference: [OASIS MQTT Version 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html).
