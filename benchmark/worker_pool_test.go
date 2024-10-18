package tests

import (
	"context"
	"testing"
	"time"

	workerpool "github.com/ichsansaid/workerpool/lib"
)

type CustomType int

func (c CustomType) Hash() int32 {
	return int32(c)
}

var taskFunc = func(ctx context.Context, input CustomType) error {
	// Simulate task work with a sleep
	time.Sleep(time.Millisecond * 500)
	return nil
}

// BenchmarkWorkerPool benchmarks the performance of worker pool.
func BenchmarkWorkerPool(b *testing.B) {
	// Set up the worker pool with a max of 4 workers and a queue size of 10
	wpool := workerpool.NewWorkerPool[CustomType](&workerpool.Config{
		MaxWorker: 10,
		MaxQueue:  10,
	})

	// Run the benchmark loop
	for n := 0; n < 5; n++ {
		// Submit multiple tasks to the worker pool in each iteration
		for i := 0; i < 10; i++ {
			wpool.SubmitTask(CustomType(i), taskFunc)
		}
	}

	// Close the worker pool after benchmarking
	wpool.Shutdown()
}
