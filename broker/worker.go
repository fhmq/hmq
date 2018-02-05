package broker

import "sync"

type Work struct {
	WorkerPool *sync.Pool
	Message    *Message
}

type Worker struct {
	WorkerChannel chan Work
}

func NewWorker() Worker {
	w := Worker{WorkerChannel: make(chan Work)}
	return w.Start()
}

func (w Worker) Start() Worker {
	go func() {
		for work := range w.WorkerChannel {
			ProcessMessage(work.Message)
			// put the worker back
			work.WorkerPool.Put(w)
		}
	}()
	return w
}
