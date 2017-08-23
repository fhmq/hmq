package broker

import "sync"

type Message struct {
	client *client
	msg    []byte
	pool   *MessagePool
}

type MessagePool struct {
	l       sync.Mutex
	maxuser int
	user    int
	queue   chan *Message
}

func (p *MessagePool) Init(num int, maxusernum int) {
	p.maxuser = maxusernum
	p.queue = make(chan *Message, num)
	for k := 0; k < num; k++ {
		m := &Message{}
		m.Pool = p
		p.Push(m)
	}
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
