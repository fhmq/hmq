package pool

import (
	"sync"

	"github.com/cespare/xxhash/v2"
)

type WorkerPool struct {
	maxWorkers      int
	taskQueue       []chan func()
	stoppedChan     chan struct{}
	submissionsDone chan struct{}
	mu              sync.Mutex
	stopped         bool
	submitters      sync.WaitGroup
	closeOnce       sync.Once
	workers         sync.WaitGroup
}

func New(maxWorkers int) *WorkerPool {
	// There must be at least one worker.
	if maxWorkers < 1 {
		maxWorkers = 1
	}

	// Each worker has a bounded queue, preserving ordering per client.
	pool := &WorkerPool{
		taskQueue:       make([]chan func(), maxWorkers),
		maxWorkers:      maxWorkers,
		stoppedChan:     make(chan struct{}),
		submissionsDone: make(chan struct{}),
	}
	// Start the task dispatcher.
	pool.dispatch()

	return pool
}

func (p *WorkerPool) Submit(uid string, task func()) bool {
	if task == nil {
		return false
	}
	p.mu.Lock()
	if p.stopped {
		p.mu.Unlock()
		return false
	}
	p.submitters.Add(1)
	p.mu.Unlock()
	defer p.submitters.Done()
	idx := xxhash.Sum64([]byte(uid)) % uint64(p.maxWorkers)
	select {
	case <-p.stoppedChan:
		return false
	case p.taskQueue[idx] <- task:
		return true
	}
}

// Close stops accepting work and waits for all accepted tasks to finish.
func (p *WorkerPool) Close() {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.stopped = true
		close(p.stoppedChan)
		p.mu.Unlock()
		p.submitters.Wait()
		close(p.submissionsDone)
		p.workers.Wait()
	})
}

func (p *WorkerPool) dispatch() {
	for i := 0; i < p.maxWorkers; i++ {
		p.taskQueue[i] = make(chan func(), 1024)
		p.workers.Add(1)
		go p.startWorker(p.taskQueue[i])
	}
}

func (p *WorkerPool) startWorker(taskChan chan func()) {
	defer p.workers.Done()
	for {
		select {
		case <-p.stoppedChan:
			<-p.submissionsDone
			for {
				select {
				case task := <-taskChan:
					task()
				default:
					return
				}
			}
		case task := <-taskChan:
			task()
		}
	}
}
