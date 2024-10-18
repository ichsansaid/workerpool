package workerpool

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type workerInput interface {
	Hash() int32 // Perubahan dari uint32 ke int32 untuk kompatibilitas dengan Load Balancer
}

type workerTask[Input workerInput] struct {
	Input   Input
	Handler func(ctx context.Context, input Input) error
}

type Worker[Input workerInput] struct {
	queue      Queue[workerTask[Input]]
	chanStop   chan struct{}
	config     *WorkerConfig
	isStarted  bool
	workerPool *WorkerPool[Input]
	mu         sync.Mutex // guards
}

func (w *Worker[Input]) ChangeState(state bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.isStarted = state
}

func (w *Worker[Input]) StartThread(ctx context.Context) error {
	if w.isStarted {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	w.isStarted = true
	goFunc := func(queue Queue[workerTask[Input]], signalStop chan struct{}, sigchan chan os.Signal, chanAck chan int32, config WorkerConfig) {
		if config.MaxWorkerIdle == 0 {
			config.MaxWorkerIdle = int64(time.Second * 10)
		}
		timer := time.NewTimer(time.Duration(config.MaxWorkerIdle))
		for run := true; run; {
			select {
			case next := <-queue.Poll():
				timer.Reset(time.Duration(config.MaxWorkerIdle))
				err := next.Handler(ctx, next.Input)
				if err != nil {
					panic(err)
				}
				if chanAck != nil {
					chanAck <- next.Input.Hash()
				}
				queue.Commit()
			case <-signalStop:
				run = false
			case <-sigchan:
				run = false
			case <-timer.C:
				if queue.Len() == 0 {
					run = false
				} else {
					timer.Reset(time.Duration(config.MaxWorkerIdle))
				}

			}
		}
		w.mu.Lock()
		w.isStarted = false
		defer w.mu.Unlock()
		close(sigchan)
	}
	if w.workerPool != nil {
		go goFunc(w.queue, w.chanStop, sigchan, w.workerPool.chanAck, *w.config)
	} else {
		go goFunc(w.queue, w.chanStop, sigchan, nil, *w.config)
	}

	return nil
}

func (w *Worker[Input]) SubmitTask(ctx context.Context, input Input, handler func(ctx context.Context, input Input) error) error {
	w.queue.Push(workerTask[Input]{
		Input:   input,
		Handler: handler,
	})
	return nil
}

func NewWorker[Input workerInput](config *WorkerConfig, pool *WorkerPool[Input]) Worker[Input] {
	if config.MaxQueue == 0 {
		config.MaxQueue = 1
	}
	return Worker[Input]{
		config:     config,
		queue:      NewBasicQueue[workerTask[Input]](config.MaxQueue),
		chanStop:   make(chan struct{}, 1),
		isStarted:  false,
		workerPool: pool,
	}
}
