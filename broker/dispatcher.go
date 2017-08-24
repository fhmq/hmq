package broker

const (
	WorkNum               = 2048
	MaxUser               = 1024 * 1024
	MessagePoolNum        = 1024
	MessagePoolUser       = MaxUser / MessagePoolNum
	MessagePoolMessageNum = MaxUser / MessagePoolNum * 4
)

type Dispatcher struct {
	WorkerPool chan chan *Message
}

func init() {
	InitMessagePool()
	dispatcher := NewDispatcher()
	dispatcher.Run()
}

func (d *Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < WorkNum; i++ {
		worker := NewWorker(d.WorkerPool)
		worker.Start()
	}
	go d.dispatch()
}

func NewDispatcher() *Dispatcher {
	pool := make(chan chan *Message, WorkNum)
	return &Dispatcher{WorkerPool: pool}
}

func (d *Dispatcher) dispatch() {
	for i := 0; i < MessagePoolNum; i++ {
		go func(idx int) {
			for {
				select {
				case msg := <-MSGPool[idx].queue:
					go func(msg *Message) {
						msgChannel := <-d.WorkerPool
						msgChannel <- msg
					}(msg)
				}
			}
		}(i)
	}

}
