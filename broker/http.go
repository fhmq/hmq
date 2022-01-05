package broker

import (
	"github.com/gin-gonic/gin"
)

func InitHTTPMoniter(b *Broker) {
	gin.SetMode(gin.ReleaseMode)
	router := gin.Default()
	router.DELETE("api/v1/connections/:clientid", func(c *gin.Context) {
		clientid := c.Param("clientid")
		cli, ok := b.clients.Load(clientid)
		if ok {
			conn, success := cli.(*client)
			if success {
				conn.Close()
			}
		}
		resp := map[string]int{
			"code": 0,
		}
		c.JSON(200, &resp)
	})

	router.Run(":" + b.config.HTTPPort)
}
