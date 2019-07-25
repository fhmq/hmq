package authhttp

import (
	"time"

	"github.com/patrickmn/go-cache"
)

type authCache struct {
	action   string
	username string
	clientID string
	password string
	topic    string
}

var (
	// cache = make(map[string]authCache)
	c = cache.New(5*time.Minute, 10*time.Minute)
)

func checkCache(action, clientID, username, password, topic string) *authCache {
	authc, found := c.Get(username)
	if found {
		return authc.(*authCache)
	}
	return nil
}

func addCache(action, clientID, username, password, topic string) {
	c.Set(username, &authCache{action: action, username: username, clientID: clientID, password: password, topic: topic}, cache.DefaultExpiration)
}
