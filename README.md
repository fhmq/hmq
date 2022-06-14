![build](https://img.shields.io/github/workflow/status/lucasvmx/hmq/Ubuntu%20build?label=Ubuntu&style=for-the-badge)
![build](https://img.shields.io/github/workflow/status/lucasvmx/hmq/MacOS%20build?label=MacOS&style=for-the-badge)
![build](https://img.shields.io/github/workflow/status/lucasvmx/hmq/Windows%20build?label=Windows&style=for-the-badge)

# Free and High Performance MQTT Broker

## About

Golang MQTT Broker, Version 3.1.1, and Compatible
for [eclipse paho client](https://github.com/eclipse?utf8=%E2%9C%93&q=mqtt&type=&language=) and mosquitto-client

## RUNNING

```bash
$ go get github.com/lucasvmx/hmq
$ cd $GOPATH/github.com/lucasvmx/hmq
$ go run main.go
```

## Usage of hmq:

```
Usage: hmq [options]

Broker Options:
    -w,  --worker <number>            Worker num to process message, perfer (client num)/10. (default 1024)
    -p,  --port <port>                Use port for clients (default: 1883)
         --host <host>                Network host to listen on. (default "0.0.0.0")
    -ws, --wsport <port>              Use port for websocket monitoring
    -wsp,--wspath <path>              Use path for websocket monitoring
    -c,  --config <file>              Configuration file

Logging Options:
    -d, --debug <bool>                Enable debugging output (default false)
    -D                                Debug enabled

Cluster Options:
    -r,  --router  <rurl>             Router who maintenance cluster info
    -cp, --clusterport <cluster-port> Cluster listen port for others

Common Options:
    -h, --help                        Show this message
```

### hmq.config

```
{
	"workerNum": 4096,
	"port": "1883",
	"host": "0.0.0.0",
	"cluster": {
		"host": "0.0.0.0",
		"port": "1993"
	},
	"router": "127.0.0.1:9888",
	"wsPort": "1888",
	"wsPath": "/ws",
	"wsTLS": true,
	"tlsPort": "8883",
	"tlsHost": "0.0.0.0",
	"tlsInfo": {
		"verify": true,
		"caFile": "tls/ca/cacert.pem",
		"certFile": "tls/server/cert.pem",
		"keyFile": "tls/server/key.pem"
	},
	"plugins": {
		"auth": "authhttp",
		"bridge": "kafka"
	}
}
```

### Features and Future

- Supports QOS 0, 1 and 2

- Cluster Support

- Containerization

- Supports retained messages

- Supports will messages

- Websocket Support

- TLS/SSL Support

- Auth Support

  - Auth Connect
  - Auth ACL
  - Cache Support

- Kafka Bridge Support

  - Action Deliver
  - Regexp Deliver

- HTTP API
  - Disconnect Connect (future more)

### Share SUBSCRIBE

```
| Prefix              | Examples                                  | Publish                      |
| ------------------- |-------------------------------------------|--------------------------- --|
| $share/<group>/topic  | mosquitto_sub -t ‘$share/<group>/topic’ | mosquitto_pub -t ‘topic’     |
```

### Cluster

```bash
 1, start router for hmq  (https://github.com/fhmq/router.git)
 	$ go get github.com/fhmq/router
 	$ cd $GOPATH/github.com/fhmq/router
 	$ go run main.go
 2, config router in hmq.config  ("router": "127.0.0.1:9888")

```

Other Version Of Cluster Based On gRPC: [click here](https://github.com/fhmq/rhmq)

### Online/Offline Notification

```bash
 topic:
     $SYS/broker/connection/clients/<clientID>
 payload:
	{"clientID":"client001","online":true/false,"timestamp":"2018-10-25T09:32:32Z"}
```

## Performance

- High throughput

- High concurrency

- Low memory and CPU

## License

- Apache License Version 2.0

## Reference

- Surgermq.(https://github.com/zentures/surgemq)

## Benchmark Tool

- https://github.com/inovex/mqtt-stresser
- https://github.com/krylovsk/mqtt-benchmark
