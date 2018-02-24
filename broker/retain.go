package broker

import (
	"github.com/eclipse/paho.mqtt.golang/packets"
	"sync"
)

type RetainList struct {
	sync.RWMutex
	root *rlevel
}
type rlevel struct {
	nodes map[string]*rnode
}
type rnode struct {
	next *rlevel
	msg  *packets.PublishPacket
}
type RetainResult struct {
	msg []*packets.PublishPacket
}

func newRNode() *rnode {
	return &rnode{}
}

func newRLevel() *rlevel {
	return &rlevel{nodes: make(map[string]*rnode)}
}

func NewRetainList() *RetainList {
	return &RetainList{root: newRLevel()}
}

func (r *RetainList) Insert(topic string, buf *packets.PublishPacket) error {

	tokens, err := PublishTopicCheckAndSpilt(topic)
	if err != nil {
		return err
	}
	// log.Info("insert tokens:", tokens)
	r.Lock()

	l := r.root
	var n *rnode
	for _, t := range tokens {
		n = l.nodes[t]
		if n == nil {
			n = newRNode()
			l.nodes[t] = n
		}
		if n.next == nil {
			n.next = newRLevel()
		}
		l = n.next
	}
	n.msg = buf
	r.Unlock()
	return nil
}

func (r *RetainList) Match(topic string) []*packets.PublishPacket {

	tokens, err := SubscribeTopicCheckAndSpilt(topic)
	if err != nil {
		return nil
	}
	results := &RetainResult{}

	r.Lock()
	l := r.root
	matchRLevel(l, tokens, results)
	r.Unlock()
	// log.Info("results: ", results)
	return results.msg

}
func matchRLevel(l *rlevel, toks []string, results *RetainResult) {
	var n *rnode
	for i, t := range toks {
		if l == nil {
			return
		}
		// log.Info("l info :", l.nodes)
		if t == "#" {
			for _, n := range l.nodes {
				n.GetAll(results)
			}
		}
		if t == "+" {
			for _, n := range l.nodes {
				if len(t[i+1:]) == 0 {
					results.msg = append(results.msg, n.msg)
				} else {
					matchRLevel(n.next, toks[i+1:], results)
				}
			}
		}

		n = l.nodes[t]
		if n != nil {
			l = n.next
		} else {
			l = nil
		}
	}
	if n != nil {
		results.msg = append(results.msg, n.msg)
	}
}

func (r *rnode) GetAll(results *RetainResult) {
	// log.Info("node 's message: ", string(r.msg))
	if r.msg != nil {
		results.msg = append(results.msg, r.msg)
	}
	l := r.next
	for _, n := range l.nodes {
		n.GetAll(results)
	}
}
