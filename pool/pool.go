package pool

// import "time"

// const (
// 	// This value is the size of the queue that workers register their
// 	// availability to the dispatcher.  There may be hundreds of workers, but
// 	// only a small channel is needed to register some of the workers.
// 	readyQueueSize = 64

// 	// If worker pool receives no new work for this period of time, then stop
// 	// a worker goroutine.
// 	idleTimeoutSec = 5
// )

// type WorkerPool struct {
// 	maxWorkers   int
// 	timeout      time.Duration
// 	taskQueue    chan func()
// 	readyWorkers chan chan func()
// 	stoppedChan  chan struct{}
// }

// func New(maxWorkers int) *WorkerPool {
// 	// There must be at least one worker.
// 	if maxWorkers < 1 {
// 		maxWorkers = 1
// 	}

// 	// taskQueue is unbuffered since items are always removed immediately.
// 	pool := &WorkerPool{
// 		taskQueue:    make(chan func()),
// 		maxWorkers:   maxWorkers,
// 		readyWorkers: make(chan chan func(), readyQueueSize),
// 		timeout:      time.Second * idleTimeoutSec,
// 		stoppedChan:  make(chan struct{}),
// 	}

// 	// Start the task dispatcher.
// 	go pool.dispatch()

// 	return pool
// }

// func (p *WorkerPool) Stop() {
// 	if p.Stopped() {
// 		return
// 	}
// 	close(p.taskQueue)
// 	<-p.stoppedChan
// }

// func (p *WorkerPool) Stopped() bool {
// 	select {
// 	case <-p.stoppedChan:
// 		return true
// 	default:
// 	}
// 	return false
// }

// func (p *WorkerPool) Submit(task func()) {
// 	if task != nil {
// 		p.taskQueue <- task
// 	}
// }

// func (p *WorkerPool) SubmitWait(task func()) {
// 	if task == nil {
// 		return
// 	}
// 	doneChan := make(chan struct{})
// 	p.taskQueue <- func() {
// 		task()
// 		close(doneChan)
// 	}
// 	<-doneChan
// }

// func (p *WorkerPool) dispatch() {
// 	defer close(p.stoppedChan)
// 	timeout := time.NewTimer(p.timeout)
// 	var workerCount int
// 	var task func()
// 	var ok bool
// 	var workerTaskChan chan func()
// 	startReady := make(chan chan func())
// Loop:
// 	for {
// 		timeout.Reset(p.timeout)
// 		select {
// 		case task, ok = <-p.taskQueue:
// 			if !ok {
// 				break Loop
// 			}
// 			// Got a task to do.
// 			select {
// 			case workerTaskChan = <-p.readyWorkers:
// 				// A worker is ready, so give task to worker.
// 				workerTaskChan <- task
// 			default:
// 				// No workers ready.
// 				// Create a new worker, if not at max.
// 				if workerCount < p.maxWorkers {
// 					workerCount++
// 					go func(t func()) {
// 						startWorker(startReady, p.readyWorkers)
// 						// Submit the task when the new worker.
// 						taskChan := <-startReady
// 						taskChan <- t
// 					}(task)
// 				} else {
// 					// Start a goroutine to submit the task when an existing
// 					// worker is ready.
// 					go func(t func()) {
// 						taskChan := <-p.readyWorkers
// 						taskChan <- t
// 					}(task)
// 				}
// 			}
// 		case <-timeout.C:
// 			// Timed out waiting for work to arrive.  Kill a ready worker.
// 			if workerCount > 0 {
// 				select {
// 				case workerTaskChan = <-p.readyWorkers:
// 					// A worker is ready, so kill.
// 					close(workerTaskChan)
// 					workerCount--
// 				default:
// 					// No work, but no ready workers.  All workers are busy.
// 				}
// 			}
// 		}
// 	}

// 	// Stop all remaining workers as they become ready.
// 	for workerCount > 0 {
// 		workerTaskChan = <-p.readyWorkers
// 		close(workerTaskChan)
// 		workerCount--
// 	}
// }

// func startWorker(startReady, readyWorkers chan chan func()) {
// 	go func() {
// 		taskChan := make(chan func())
// 		var task func()
// 		var ok bool
// 		// Register availability on starReady channel.
// 		startReady <- taskChan
// 		for {
// 			// Read task from dispatcher.
// 			task, ok = <-taskChan
// 			if !ok {
// 				// Dispatcher has told worker to stop.
// 				break
// 			}

// 			// Execute the task.
// 			task()

// 			// Register availability on readyWorkers channel.
// 			readyWorkers <- taskChan
// 		}
// 	}()
// }
