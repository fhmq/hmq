package broker

import "sync"

type Message struct {
	client *client
	msg    []byte
	// pool   *MessagePool
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

func init() []MessagePool {
	MSGPool = make([]MessagePool, (MessagePoolNum + 2))
	for i := 0; i < (MessagePoolNum + 2); i++ {
		MSGPool[i].Init(MessagePoolUser, MessagePoolMessageNum)
	}
	return MSGPool
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

func (p *MessagePool) Pop() *Message {

	p2 := <-p.queue
	return p2
}

func (p *MessagePool) Push(pmessage *Message) {

	p.queue <- pmessage
}
