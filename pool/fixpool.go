package pool

import (
	"context"
	"github.com/segmentio/fasthash/fnv1a"
)

type WorkerPool struct {
	maxWorkers  int
	taskQueue   []chan func()
	stoppedChan chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
}

func New(maxWorkers int) *WorkerPool {
	// There must be at least one worker.
	if maxWorkers < 1 {
		maxWorkers = 1
	}

	ctx, cancel := context.WithCancel(context.Background())

	// taskQueue is unbuffered since items are always removed immediately.
	pool := &WorkerPool{
		taskQueue:   make([]chan func(), maxWorkers),
		maxWorkers:  maxWorkers,
		stoppedChan: make(chan struct{}),
		ctx:         ctx,
		cancel:      cancel,
	}
	// Start the task dispatcher.
	pool.dispatch()

	return pool
}

func (p *WorkerPool) Submit(uid string, task func()) {
	idx := fnv1a.HashString64(uid) % uint64(p.maxWorkers)
	if task != nil {
		p.taskQueue[idx] <- task
	}
}

func (p *WorkerPool) Stop() {
	p.cancel()
}

func (p *WorkerPool) dispatch() {
	for i := 0; i < p.maxWorkers; i++ {
		p.taskQueue[i] = make(chan func())
		go startWorker(p.taskQueue[i], p.ctx)
	}
}

func startWorker(taskChan chan func(), ctx context.Context) {
	go func() {
		var task func()
		for {
			select {
			case task = <-taskChan:
				// Execute the task.
				task()
			case <-ctx.Done():
				return
			}
		}
	}()
}
