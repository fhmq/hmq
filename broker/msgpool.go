package broker

import "sync"

type Message struct {
	client *client
	msg    []byte
}

var (
	MSGPool []MessagePool
)

type MessagePool struct {
	l       sync.Mutex
	maxuser int
	user    int
	queue   chan *Message
}

func InitMessagePool() {
	MSGPool = make([]MessagePool, MessagePoolNum)
	for i := 0; i < MessagePoolNum; i++ {
		MSGPool[i].Init(MessagePoolUser, MessagePoolMessageNum)
	}
}

func (p *MessagePool) Init(num int, maxusernum int) {
	p.maxuser = maxusernum
	p.queue = make(chan *Message, num)
}

func (p *MessagePool) GetPool() *MessagePool {
	p.l.Lock()
	if p.user+1 < p.maxuser {
		p.user += 1
		p.l.Unlock()
		return p
	} else {
		p.l.Unlock()
		return nil
	}

}

func (p *MessagePool) Reduce() {
	p.l.Lock()
	p.user -= 1
	p.l.Unlock()

}
