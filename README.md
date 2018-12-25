Free and High Performance MQTT Broker 
============

## About
Golang MQTT Broker, Version 3.1.1, and Compatible
for [eclipse paho client](https://github.com/eclipse?utf8=%E2%9C%93&q=mqtt&type=&language=) and mosquitto-client

Download: [click here](https://github.com/fhmq/hmq/releases)

## RUNNING
```bash
$ go get github.com/fhmq/hmq
$ cd $GOPATH/github.com/fhmq/hmq
$ go run main.go
```

## Usage of hmq:
~~~
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
~~~

### hmq.config
~~~
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

* Websocket Support

* TLS/SSL Support

* Flexible  ACL

### Cluster
```bash
 1, start router for hmq  (https://github.com/fhmq/router.git)
 	$ go get github.com/fhmq/router
 	$ cd $GOPATH/github.com/fhmq/router
 	$ go run main.go
 2, config router in hmq.config  ("router": "127.0.0.1:9888")
 
```

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
