package tests

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	workerpool "github.com/ichsansaid/workerpool/lib"
)

type CustomType int

func (c CustomType) Hash() int32 {
	return int32(c)
}

func TestWorkerPool(b *testing.T) {
	// Set up the worker pool with a max of 4 workers and a queue size of 10
	wpool := workerpool.NewWorkerPool[CustomType](&workerpool.Config{
		MaxWorker:     4,
		MaxQueue:      10,
		MaxWorkerIdle: int64(time.Second * 5),
	})
	totalDone := 0
	// Define the task function
	taskFunc := func(ctx context.Context, input CustomType) error {
		// Simulate task work with a sleep
		fmt.Println("Done")
		time.Sleep(time.Second * 1)
		return nil
	}

	// Submit multiple tasks to the worker pool in each iteration
	for i := 0; i < 10; i++ {
		wpool.SubmitTask(CustomType(i), taskFunc)
	}

	time.Sleep(time.Second * 10)
	fmt.Println("----------------------------------------------------------------")
	// Submit multiple tasks to the worker pool in each iteration
	for i := 0; i < 10; i++ {
		wpool.SubmitTask(CustomType(i), taskFunc)
	}

	// Close the worker pool after benchmarking
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	for {
		select {
		case <-sigChan:
			fmt.Printf("Total Done %d\n", totalDone)
			wpool.Shutdown()
			return
		}
	}
}
