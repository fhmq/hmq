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
			conn, succss := cli.(*client)
			if succss {
				conn.Close()
			}
		}
		resp := map[string]int{
			"code": 0,
		}
		c.JSON(200, &resp)
	})

	router.GET("api/v1/nodes", func(c *gin.Context) {
		num := 0
		b.clients.Range(func(key, value interface{}) bool {
			num++
			return true
		})
		resp := map[string]int{
			"code":   0,
			"counts": num,
		}
		c.JSON(200, &resp)
	})

	router.Run(":8080")
}
