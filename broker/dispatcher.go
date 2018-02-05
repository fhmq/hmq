package broker

import (
	"sync"
)

// Dispatcher will delegate ProcessMessage func to multiple goroutines
type Dispatcher struct {
	workerPool *sync.Pool
}

// NewDispatcher create a *Dispatcher instance
func NewDispatcher() *Dispatcher {
	return &Dispatcher{workerPool: &sync.Pool{
		New: func() interface{} {
			return NewWorker()
		},
	},
	}
}

// Dispatch a message to the workers
func (d *Dispatcher) Dispatch(message *Message) {
	d.workerPool.Get().(Worker).WorkerChannel <- Work{WorkerPool: d.workerPool, Message: message}
}
