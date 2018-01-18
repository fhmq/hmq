Free and High Performance MQTT Broker 
============

## About
Golang MQTT Broker, Version 3.1.1, and Compatible
for [eclipse paho client](https://github.com/eclipse?utf8=%E2%9C%93&q=mqtt&type=&language=)

Download: [click here](https://github.com/fhmq/hmq/releases)

## RUNNING
```bash
$ git clone https://github.com/fhmq/hmq.git
$ cd hmq
$ go run main.go
```

### broker.config
~~~
{
	"workerNum": 4096,
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

* Supports QOS 0 and 1

* Cluster Support

* Containerization

* Supports retained messages

* Supports will messages  

* Queue subscribe

* Websocket Support

* TLS/SSL Support

* Flexible  ACL

### QUEUE SUBSCRIBE
~~~
| Prefix        | Examples                        |
| ------------- |---------------------------------|
| $queue/       | mosquitto_sub -t ‘$queue/topic’ |
~~~

### ACL Configure
#### The ACL rules define:
~~~
Allow | type | value | pubsub | Topics
~~~
#### ACL Config
~~~
## type clientid , username, ipaddr
##pub 1 ,  sub 2,  pubsub 3
## %c is clientid , %u is username
allow      ip          127.0.0.1   2     $SYS/#
allow      clientid    0001        3     #
allow      username    admin       3     #
allow      username    joy         3     /test,hello/world 
allow      clientid    *           1     toCloud/%c
allow      username    *           1     toCloud/%u
deny       clientid    *           3     #
~~~

~~~
#allow local sub $SYS topic
allow      ip          127.0.0.1   2    $SYS/#
~~~
~~~
#allow client who's id with 0001 or username with admin pub sub all topic
allow      clientid    0001        3        #
allow      username    admin       3        #
~~~
~~~
#allow client with the username joy can pub sub topic '/test' and 'hello/world'
allow      username    joy         3     /test,hello/world 
~~~
~~~
#allow all client pub the topic toCloud/{clientid/username}
allow      clientid    *         1         toCloud/%c
allow      username    *         1         toCloud/%u
~~~
~~~
#deny all client pub sub all topic
deny       clientid    *         3           #
~~~
Client match acl rule one by one
~~~
          ---------              ---------              ---------
Client -> | Rule1 | --nomatch--> | Rule2 | --nomatch--> | Rule3 | --> 
          ---------              ---------              ---------
              |                      |                      |
            match                  match                  match
             \|/                    \|/                    \|/
        allow | deny           allow | deny           allow | deny
~~~

## Performance

* High throughput

* High concurrency

* Low memory and CPU


## License

* Apache License Version 2.0
