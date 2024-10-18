package workerpool

import (
	"sync"
)

// Queue is an interface for a generic queue.
type Queue[T any] interface {
	Poll() chan T // Changed return type to return item and a boolean indicating success
	Push(item T)
	Commit() // No parameters, just mark the last polled item as committed
	Len() int
}

// BasicQueue is a thread-safe queue implementation using channels.
type BasicQueue[T any] struct {
	queue      chan T        // Channel to hold items
	lastPolled chan T        // Channel to hold the last polled item
	commitCh   chan struct{} // Channel to signal when the last item is committed
	mu         sync.Mutex    // Mutex for thread-safe operations
}

// NewBasicQueue creates a new BasicQueue instance with a specified size.
func NewBasicQueue[T any](size int) Queue[T] {
	return &BasicQueue[T]{
		queue:      make(chan T, size),     // Initialize channel with capacity
		lastPolled: make(chan T, 1),        // Buffered channel to hold last polled item
		commitCh:   make(chan struct{}, 1), // Buffered channel for commit signaling
	}
}

func (b *BasicQueue[T]) Len() int { return len(b.queue) }

// Push adds an item to the queue, blocks if the queue is full.
func (b *BasicQueue[T]) Push(item T) {
	b.queue <- item // Push to the channel (blocks if full)
}

// Poll retrieves and returns the next item from the queue.
// This method blocks if the previous item has not been committed.
func (b *BasicQueue[T]) Poll() chan T {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Wait for the previous item to be committed if it exists
	select {
	case <-b.commitCh: // Wait for a commit signal
	default: // No previous item to wait for
	}

	return b.queue
}

// Commit marks the last polled item as committed.
func (b *BasicQueue[T]) Commit() {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Signal that the last polled item has been committed
	b.commitCh <- struct{}{}
}
