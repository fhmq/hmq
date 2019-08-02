package broker

import (
	"context"
	"strings"
	"time"

	"go.uber.org/zap"

	"go.etcd.io/etcd/clientv3"
)

func (b *Broker) ConnectToEtcd() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{"http://106.75.231.3:2379"},
		DialTimeout: time.Second * 5,
	})
	if err != nil {
		panic(err)
	}
	b.etcdClient = cli
	b.GetNode()

	rch := cli.Watch(context.Background(), "root/node/", clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				keys := strings.Split(string(ev.Kv.Key), "/")
				nodeID := keys[len(keys)-1]
				nodeURL := string(ev.Kv.Value)
				log.Debug("new node join: ", zap.String("nodeID", nodeID), zap.String("nodeIP", nodeURL))
				b.AddNode(nodeID, nodeURL)
			case clientv3.EventTypeDelete:
				keys := strings.Split(string(ev.Kv.Key), "/")
				nodeID := keys[len(keys)-1]
				log.Debug("node left: ", zap.String("nodeID", nodeID))
				b.DeleteNode(nodeID)
			default:
				continue
			}
		}
	}
}

func (b *Broker) GetNode() {
	resp, err := b.etcdClient.Get(context.Background(),
		"root/node/", clientv3.WithPrefix())
	if err != nil {
		log.Error("get nodes from etcd error", zap.Error(err))
	}
	for _, kv := range resp.Kvs {
		keys := strings.Split(string(kv.Key), "/")
		nodeID := keys[len(keys)-1]
		nodeURL := string(kv.Value)
		b.AddNode(nodeID, nodeURL)
	}
}

func (b *Broker) CloseSelf() {
	_, err := b.etcdClient.Delete(context.Background(),
		"root/node/"+b.id)
	if err != nil {
		log.Error("delete self from etcd error", zap.Error(err))
	}
}

func (b *Broker) PutSelf() {
	b.etcdClient.Txn()
	_, err := b.etcdClient.Put(context.Background(),
		"root/node/"+b.id, b.config.RpcPort)
	if err != nil {
		log.Error("delete self from etcd error", zap.Error(err))
	}
}

func (b *Broker) AddNode(id, url string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if id == b.id {
		return
	}
	_, exist := b.nodes[id]
	if !exist {
		b.nodes[id] = url
		go b.initRPCClient(id, url)
	}
}

func (b *Broker) DeleteNode(id string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.nodes, id)
}
