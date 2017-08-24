package broker

type Worker struct {
	WorkerPool chan chan *Message
	MsgChannel chan *Message
	quit       chan bool
}

func NewWorker(workerPool chan chan *Message) Worker {
	return Worker{
		WorkerPool: workerPool,
		MsgChannel: make(chan *Message),
		quit:       make(chan bool)}
}

func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.WorkerPool <- w.MsgChannel
			select {
			case msg := <-w.MsgChannel:
				// we have received a work request.
				ProcessMessage(msg)
			case <-w.quit:
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}
