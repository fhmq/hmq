
Free and High Performance MQTT Broker 
============

## About
Golang MQTT Broker, Version 3.1.1, and Compatible
for [eclipse paho client](https://github.com/eclipse?utf8=%E2%9C%93&q=mqtt&type=&language=) and mosquitto-client

## RUNNING
Use Go 1.27.3 or newer to build from source:

The v3 Go module path is `github.com/fhmq/hmq/v3`; previous unversioned imports
must be updated. Release binaries do not require a Go installation.

```bash
git clone https://github.com/fhmq/hmq.git
cd hmq
go build -o hmq .
./hmq --version
./hmq --config conf/hmq.config
```

Release archives contain the executable, license, sample configuration and
operational documentation. On Windows use `hmq.exe`. The sample listens only
on `127.0.0.1:1883`, disables TLS/HTTP/WebSocket ports and uses permissive mock
authentication for local evaluation. Configure real authentication and TLS
before exposing the broker to a network; no private keys are bundled.

## Usage of hmq:
~~~
Usage: hmq [options]

Broker Options:
    -w,  --worker <number>            Worker num to process message, perfer (client num)/10. (default 1024)
    -p,  --port <port>                Use port for clients (default: 1883)
         --host <host>                Network host to listen on. (default "127.0.0.1")
    -ws, --wsport <port>              Use port for websocket monitoring
    -wsp,--wspath <path>              Use path for websocket monitoring
    -c,  --config <file>              Configuration file

Logging Options:
    -d, --debug <bool>                Enable debugging output (default false)
    -D                                Debug enabled

Common Options:
         --version                    Show release version and commit
    -h, --help                        Show this message
~~~

### hmq.config
~~~
{
	"workerNum": 1024,
	"port": "1883",
	"host": "127.0.0.1",
	"wsPort": "",
	"wsPath": "/ws",
	"wsTLS": false,
	"tlsPort": "",
	"plugins": {
		"auth": "mock",
		"bridge": "none"
	}
}
~~~

### Features and Future

* Default memory mode supports QoS 0, 1 and 2; experimental modes have explicit restrictions.

* Cluster Support

* Containerization

* Supports retained messages

* Supports will messages  

* Websocket Support

* TLS/SSL Support

* Auth Support
	* Auth Connect
	* Auth ACL
	* Cache Support

* Kafka Bridge Support
	* Action Deliver
	* Regexp Deliver

* HTTP API
	* Disconnect Connect (future more)

### Share SUBSCRIBE
~~~
| Prefix              | Examples                                  | Publish                      |
| ------------------- |-------------------------------------------|--------------------------- --|
| $share/<group>/topic  | mosquitto_sub -t ‘$share/<group>/topic’ | mosquitto_pub -t ‘topic’     |
~~~

### Cluster

Nodes connect directly without a router. See [cluster configuration](conf/cluster.md)
for the required static peer topology, mutual TLS and node identities. The current
implementation offers three explicit modes:

* Default direct forwarding is best effort, without durable replay.
* Experimental reliable mode adds local event/inbox logs and retrying receipt
  acknowledgments, but no consensus or replicated storage.
* Experimental quorum mode embeds one fixed Raft partition with majority-commit
  publication acceptance and authenticated follower-to-leader forwarding. It
  requires no external coordinator. It is not a full Kafka implementation.

Quorum currently supports ordinary non-retained QoS 0/1 publications and clean,
online sessions only. QoS 2, retained state, shared subscriptions, bridges and
replicated/offline MQTT sessions are not supported in this mode. A successful
PUBACK confirms the publication log, not subscriber receipt. Timeouts and client
retries can still produce duplicate deliveries. Static full-mesh cluster seeds
are required for forwarding to any elected leader. See [quorum runtime notes](broker/QUORUM.md)
for limits, recovery rules and failure semantics before enabling it.

Legacy `router` and cluster `host`/`port` configuration is rejected. Migrate to
the documented `cluster.listen`, seeds, node identities and TLS settings; upgrade
all members together. Durable logs are identity-bound and must not be reset or
mixed independently. There is no automatic migration from the old router or
memory-only sessions into the new logs.

### Online/Offline Notification
```bash
 topic:
     $SYS/broker/connection/clients/<clientID>
 payload:
	{"clientID":"client001","online":true/false,"timestamp":"2018-10-25T09:32:32Z"}
```

## Performance

* High throughput

* High concurrency

* Low memory and CPU


## License

* Apache License Version 2.0


## Reference

* Surgermq.(https://github.com/zentures/surgemq)

## Benchmark Tool

* https://github.com/inovex/mqtt-stresser
* https://github.com/krylovsk/mqtt-benchmark
# Local retained persistence

By default retained messages are kept in memory. Set the JSON configuration
field `retainedLogPath` to a nonempty local log file path to restore retained
messages on restart, for example `"retainedLogPath": "/var/lib/hmq/retained.log"`.
The parent directory must already exist. Invalid paths, lock conflicts, corrupt
logs, and unsupported platforms prevent broker creation; there is no fallback
to memory. Linux and macOS are supported, subject to the filesystem and sync
limitations in [storage guarantees](broker/storage/README.md).

This is local storage, not replicated or consensus-backed storage. Cluster
message forwarding does not provide durable replication or reconciliation.
Only retained messages are persisted, not subscriptions or sessions. For a
retained QoS 1 or 2 publish handled by the broker, the local retained update is
written before PUBACK or PUBREC; a storage error closes the connection without
that success acknowledgement. This does not guarantee cluster replication or
delivery to subscribers, and a bridge can consume a publish before this path.
