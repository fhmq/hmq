Free and High Performance MQTT Broker 
============

## About
Golang MQTT Broker, Version 3.1.1, and Compatible
for [eclipse paho client](https://github.com/eclipse?utf8=%E2%9C%93&q=mqtt&type=&language=)

## RUNNING
```bash
$ git clone https://github.com/fhmq/fhmq.git
$ cd fhmq
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
		"routers": ["192.168.10.11:1993","192.168.10.12:1993"]
	}
}
~~~

### Features and Future

* Supports QOS 0

* Cluster Support

* Supports retained messages

* Supports will messages  

* Queue subscribe

### QUEUE SUBSCRIBE

| Prefix        | Examples                        |
| ------------- |---------------------------------|
| $queue/       | mosquitto_sub -t ‘$queue/topic’ |
