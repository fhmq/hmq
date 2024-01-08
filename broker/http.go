package broker

import (
	"github.com/gin-gonic/gin"
)

const (
	CONNECTIONS	= "api/v1/connections"
)

type ConnClient struct {
	Info				`json:"info"`
	LastMsgTime	int64		`json:"lastMsg"`
}

type resp struct {
	Code	int		`json:"code,omitempty"`
	Clients	[]ConnClient	`json:"clients,omitempty"`
}

func InitHTTPMoniter(b *Broker) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.DELETE(CONNECTIONS + "/:clientid", func(c *gin.Context) {
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
		b.clients.Range(func (k, v interface{}) bool {
			cl, _ := v.(*client)
			var pubPack = PubPacket{}
			if cl.info.willMsg != nil {
				pubPack.TopicName = cl.info.willMsg.TopicName
				pubPack.Payload = cl.info.willMsg.Payload
			}

			msg := ConnClient{
				Info: Info{
					ClientID: cl.info.clientID,
					Username: cl.info.username,
					Password: cl.info.password,
					Keepalive: cl.info.keepalive,
					WillMsg: pubPack,
				},
				LastMsgTime: cl.lastMsgTime,
			}

			conns = append(conns, msg)
			return true
		})
		r := resp{Clients: conns}
		c.JSON(200, &r)
	})

	router.Run(":" + b.config.HTTPPort)
}
