package workerpool

import (
	"context"
	"time"
)

type WorkerPool[Input workerInput] struct {
	workers      []*Worker[Input]
	loadBalancer LoadBalancer // Tambahkan Load Balancer
	chanAck      chan int32
	ackThreadRun bool
}

func (p *WorkerPool[Input]) Ack(key int32) {
	p.chanAck <- key
}

func (p *WorkerPool[Input]) startAckThread() {
	if !p.ackThreadRun {
		go func() {
			for {
				select {
				case ack := <-p.chanAck:
					p.loadBalancer.Ack(ack) // Gunakan Load Balancer
				}
			}
		}()
		p.ackThreadRun = true
	}

}

// GetWorker digunakan untuk mendapatkan worker yang sesuai menggunakan Load Balancer
func (p *WorkerPool[Input]) GetWorker(input Input) (*Worker[Input], error) {
	workerNum := p.loadBalancer.Next(input.Hash())
	worker := p.workers[workerNum]
	return worker, nil
}

// SubmitTask menerima task, memilih worker menggunakan Load Balancer, dan mendistribusikan task ke worker yang terpilih
func (p *WorkerPool[Input]) SubmitTask(input Input, handler func(ctx context.Context, input Input) error) error {
	if !p.ackThreadRun {
		p.startAckThread()
	}
	worker, err := p.GetWorker(input)
	if err != nil {
		return err
	}
	if !worker.isStarted {
		worker.StartThread(context.Background())
	}
	return worker.SubmitTask(context.Background(), input, handler)
}

func (p *WorkerPool[Input]) Shutdown() {
	for _, worker := range p.workers {
		worker.chanStop <- struct{}{}
		worker.isStarted = false
	}
}

// NewWorkerPool membuat instance WorkerPool baru dan menginisialisasi Load Balancer
func NewWorkerPool[Input workerInput](cfg *Config) *WorkerPool[Input] {
	if cfg.MaxWorkerIdle == 0 {
		cfg.MaxWorkerIdle = 3000
	}
	pool := &WorkerPool[Input]{
		workers: make([]*Worker[Input], cfg.MaxWorker),
		loadBalancer: NewStickyLeastRoundRobinLoadBalancer(
			&StickyLeastRoundRobinLbConfig{
				Size:       cfg.MaxWorker,
				GcInterval: time.Duration(cfg.MaxWorkerIdle),
			},
		), // Set Load Balancer
		chanAck: make(chan int32, cfg.MaxWorker*2),
	}
	for i := 0; i < cfg.MaxWorker; i++ {
		worker := NewWorker(&WorkerConfig{
			MaxWorkerIdle: cfg.MaxWorkerIdle,
			MaxQueue:      cfg.MaxQueue,
		}, pool)
		pool.workers[i] = &worker
	}
	return pool
}
