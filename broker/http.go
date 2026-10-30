package broker

import (
	"errors"
	"github.com/fhmq/hmq/v3/broker/cluster"
	"github.com/gin-gonic/gin"
	"net"
	"net/http"

	"go.uber.org/zap"
)

const (
	CONNECTIONS = "api/v1/connections"
)

type ConnClient struct {
	Info        `json:"info"`
	LastMsgTime int64 `json:"lastMsg"`
}

type resp struct {
	Code    int          `json:"code,omitempty"`
	Clients []ConnClient `json:"clients,omitempty"`
}

func InitHTTPMoniter(b *Broker) {
	gin.SetMode(gin.ReleaseMode)
	router := monitorRouter(b)

	l, err := net.Listen("tcp", ":"+b.config.HTTPPort)
	if err != nil {
		log.Error("monitor HTTP listen failed", zap.Error(err))
		return
	}
	if !b.registerListener(l) {
		return
	}
	s := &http.Server{Handler: b.trackHTTP(router)}
	if !b.registerServer(s) {
		l.Close()
		return
	}
	if err := s.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("monitor HTTP server failed", zap.Error(err))
	}
}

func monitorRouter(b *Broker) *gin.Engine {
	router := gin.Default()
	router.GET("api/v1/cluster/quorum", func(c *gin.Context) {
		status := quorumHealth{State: "disabled", Partitions: 1, Routing: "disabled"}
		if b.quorumRuntime != nil {
			status = b.quorumRuntime.health()
		}
		c.JSON(http.StatusOK, status)
	})
	router.GET("api/v1/cluster", func(c *gin.Context) {
		status := cluster.Status{State: "disabled", Peers: []cluster.PeerStatus{}}
		if b.clusterNode != nil {
			status = b.clusterNode.Status()
		}
		c.JSON(http.StatusOK, status)
	})
	router.GET("api/v1/cluster/reliable", func(c *gin.Context) {
		status := reliableHealth{State: "disabled"}
		if b.reliableRuntime != nil {
			status = b.reliableRuntime.health()
		}
		c.JSON(http.StatusOK, status)
	})
	router.DELETE(CONNECTIONS+"/:clientid", func(c *gin.Context) {
		clientid := c.Param("clientid")
		cli, ok := b.clients.Load(clientid)
		if ok {
			conn, success := cli.(*client)
			if success {
				conn.Close()
			}
		}
		r := resp{Code: 0}
		c.JSON(200, &r)
	})
	router.GET(CONNECTIONS, func(c *gin.Context) {
		conns := make([]ConnClient, 0)
		b.clients.Range(func(k, v interface{}) bool {
			cl, ok := v.(*client)
			if !ok || cl == nil {
				return true
			}
			var pubPack = PubPacket{}
			cl.stateMu.Lock()
			if cl.info.willMsg != nil {
				pubPack.TopicName = cl.info.willMsg.TopicName
				pubPack.Payload = append([]byte(nil), cl.info.willMsg.Payload...)
			}
			lastMsgTime := cl.lastMsgTime
			cl.stateMu.Unlock()

			msg := ConnClient{
				Info: Info{
					ClientID:  cl.info.clientID,
					Username:  cl.info.username,
					Keepalive: cl.info.keepalive,
					WillMsg:   pubPack,
				},
				LastMsgTime: lastMsgTime,
			}

			conns = append(conns, msg)
			return true
		})
		r := resp{Clients: conns}
		c.JSON(200, &r)
	})

	return router
}
