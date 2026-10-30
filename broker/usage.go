package broker

var usageStr = `
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
    -D                                Debug and trace

Cluster Options:
    Configure direct peers in the configuration file; see conf/cluster.md.

Common Options:
         --version                    Show release version and commit
    -h, --help                        Show this message
`
