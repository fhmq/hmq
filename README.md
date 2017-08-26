Free and High Performance MQTT Broker 
============

## About
Golang MQTT Broker, Version 3.1.1, and Compatible
for [eclipse paho client](https://github.com/eclipse?utf8=%E2%9C%93&q=mqtt&type=&language=)

## RUNNING
```bash
$ git clone https://github.com/fhmq/hmq.git
$ cd hmq
$ go run main.go
```

### broker.config
~~~
{
	"port": "1883",
	"host": "0.0.0.0",
	"cluster": {
		"host": "0.0.0.0",
		"port": "1993",
		"routers": ["10.10.0.11:1993","10.10.0.12:1993"]
	},
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
	"acl":true,
	"aclConf":"conf/acl.conf"
}
~~~

### Features and Future

* Supports QOS 0

* Cluster Support

* Supports retained messages

* Supports will messages  

* Queue subscribe

* Websocket Support

* TLS/SSL Support

### QUEUE SUBSCRIBE

| Prefix        | Examples                        |
| ------------- |---------------------------------|
| $queue/       | mosquitto_sub -t ‘$queue/topic’ |
