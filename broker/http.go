package broker

import (
	"github.com/gin-gonic/gin"
)

const (
	CONNECTIONS	= "api/v1/connections"
)

type resp struct {
	Code	int		`json:"code,omitempty"`
	Clients	[]string	`json:"clients,omitempty"`
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
		conns := make([]string, 0)
		b.clients.Range(func (k, v interface{}) bool {
			conns = append(conns, v.(*client).info.clientID)
			return true
		})
		r := resp{Clients: conns}
		c.JSON(200, &r)
	})

	router.Run(":" + b.config.HTTPPort)
}
