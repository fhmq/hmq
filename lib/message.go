package lib

import "sync"

type MessagePool struct {
	sync.Mutex
	queue chan *Message
}

func (p *MessagePool) Init(len int, maxusernum int) {
	p.maxuser = maxusernum
	p.queue = make(chan *Message, len)
}
