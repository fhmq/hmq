package broker

const (
	WorkPoolNum           = 1024
	MaxUser               = 1024 * 1024
	MessagePoolNum        = 1024
	MessagePoolUser       = MaxUser / MessagePoolNum
	MessagePoolMessageNum = MaxUser / MessagePoolNum * 4

	// MessageBoxNum           = 256
	// MessageBoxUserNum       = MaxUser / MessageBoxNum
	// MessageBoxMessageLength = MessageBoxUserNum
)

var (
	MyDispatcher Dispatcher
)

type Dispatcher struct {
	WorkerPool chan chan *Message
}

func init() {
	workerPool = make(chan chan *Message, WorkPoolNum)
	MyDispatcher = &Dispatcher{WorkerPool: workerPool}
}

func (d *Dispatcher) dispatch() {
	for i := 0; i < MessagePoolNum; i++ {
		go func(idx int) {
			for {
				select {
				case msg := <-MSGPool[idx].Pop():
					go func(msg *Message) {
						msgChannel := <-d.WorkerPool
						msgChannel <- msg
					}(msg)
				}
			}
		}(i)
	}

}
