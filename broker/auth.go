/* Copyright (c) 2018, joy.zhou <chowyu08@gmail.com>
 */
package broker

import (
	"strings"

	"github.com/fhmq/hmq/lib/acl"

	"go.uber.org/zap"

	"github.com/fsnotify/fsnotify"
)

const (
	PUB = 1
	SUB = 2
)

func (c *client) CheckTopicAuth(typ int, topic string) bool {
	if c.typ != CLIENT || !c.broker.config.Acl {
		return true
	}
	if strings.HasPrefix(topic, "$queue/") {
		topic = string([]byte(topic)[7:])
		if topic == "" {
			return false
		}
	}
	ip := c.info.remoteIP
	username := string(c.info.username)
	clientid := string(c.info.clientID)
	aclInfo := c.broker.AclConfig
	return acl.CheckTopicAuth(aclInfo, typ, ip, username, clientid, topic)

}

var (
	watchList = []string{"./conf"}
)

func (b *Broker) handleFsEvent(event fsnotify.Event) error {
	switch event.Name {
	case b.config.AclConf:
		if event.Op&fsnotify.Write == fsnotify.Write ||
			event.Op&fsnotify.Create == fsnotify.Create {
			log.Info("text:handling acl config change event:", zap.String("filename", event.Name))
			aclconfig, err := acl.AclConfigLoad(event.Name)
			if err != nil {
				log.Error("aclconfig change failed, load acl conf error: ", zap.Error(err))
				return err
			}
			b.AclConfig = aclconfig
		}
	}
	return nil
}

func (b *Broker) StartAclWatcher() {
	go func() {
		wch, e := fsnotify.NewWatcher()
		if e != nil {
			log.Error("start monitor acl config file error,", zap.Error(e))
			return
		}
		defer wch.Close()

		for _, i := range watchList {
			if err := wch.Add(i); err != nil {
				log.Error("start monitor acl config file error,", zap.Error(err))
				return
			}
		}
		log.Info("watching acl config file change...")
		for {
			select {
			case evt := <-wch.Events:
				b.handleFsEvent(evt)
			case err := <-wch.Errors:
				log.Error("error:", zap.Error(err))
			}
		}
	}()
}
