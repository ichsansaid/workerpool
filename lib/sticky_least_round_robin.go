package workerpool

import (
	"sync"
	"time"
)

type StickyLeastRoundRobinLbConfig struct {
	Size       int
	GcInterval time.Duration
}

type stateCountUsed struct {
	mu        sync.Mutex
	countUsed []int
}

// Mengurangi count dari thread
func (s *stateCountUsed) Subtract(key int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.countUsed[int(key)]--
}

// Menambah count dari thread
func (s *stateCountUsed) Increase(key int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.countUsed[int(key)]++
}

// Menghapus mapping dari thread
func (s *stateCountUsed) Delete(key int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.countUsed[key] = 0
}

// Mendapatkan index dari thread dengan jumlah tugas paling sedikit
func (s *stateCountUsed) GetLeastUsedThread() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	var leastUsedKey int = -1
	minCount := int(^uint(0) >> 1) // Set to max int value

	// Cari thread dengan jumlah task paling sedikit
	for key, count := range s.countUsed {
		if count < minCount {
			leastUsedKey = key
			minCount = count
		}
	}
	return leastUsedKey
}

type StickyLeastRoundRobinLoadBalancer struct {
	size           int
	index          int
	mu             sync.Mutex
	keys           map[int32]int
	lastUsed       map[int32]time.Time
	stateCountUsed stateCountUsed
	gcInterval     time.Duration
	stopGC         chan struct{}
}

// Ack implements workerpool.LoadBalancer.
// Mengurangi jumlah beban di thread terkait
func (r *StickyLeastRoundRobinLoadBalancer) Ack(key int32) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stateCountUsed.Subtract(r.keys[key])
}

// Menentukan thread berikutnya untuk kunci tertentu.
// Jika kunci sudah ada, gunakan thread yang sama. Jika tidak, pilih thread dengan beban terendah (Least Round Robin)
func (r *StickyLeastRoundRobinLoadBalancer) Next(key int32) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.size == 0 {
		return -1
	}

	// Sticky Round Robin: jika kunci sudah ada, tetap gunakan thread yang sama
	if idx, exists := r.keys[key]; exists {

		r.lastUsed[key] = time.Now()
		return idx
	}

	// Least Round Robin: pilih thread dengan beban terendah
	selectedIndex := int(r.stateCountUsed.GetLeastUsedThread())

	// Rekam thread yang digunakan untuk kunci baru
	r.keys[key] = selectedIndex
	r.lastUsed[key] = time.Now()
	r.stateCountUsed.Increase(r.keys[key])
	return selectedIndex
}

// Garbage collector untuk membersihkan kunci yang tidak digunakan setelah beberapa waktu
func (r *StickyLeastRoundRobinLoadBalancer) startGarbageCollector() {
	ticker := time.NewTicker(r.gcInterval)
	go func() {
		for run := true; run; {
			select {
			case <-ticker.C:
				r.mu.Lock()
				now := time.Now()
				for key, lastUsed := range r.lastUsed {
					if now.Sub(lastUsed) > r.gcInterval {
						delete(r.keys, key)
						delete(r.lastUsed, key)
						r.stateCountUsed.Delete(r.keys[key])
					}
				}
				r.mu.Unlock()
			case <-r.stopGC:
				ticker.Stop()
				run = false
			}
		}
		r.StopGarbageCollector()
	}()
}

// Berhentikan garbage collector
func (r *StickyLeastRoundRobinLoadBalancer) StopGarbageCollector() {
	close(r.stopGC)
}

// Buat instance baru dari StickyLeastRoundRobinLoadBalancer
func NewStickyLeastRoundRobinLoadBalancer(cfg *StickyLeastRoundRobinLbConfig) LoadBalancer {
	lb := &StickyLeastRoundRobinLoadBalancer{
		size:       cfg.Size,
		index:      0,
		keys:       make(map[int32]int),
		lastUsed:   make(map[int32]time.Time),
		gcInterval: cfg.GcInterval,
		stopGC:     make(chan struct{}),
		stateCountUsed: stateCountUsed{
			mu:        sync.Mutex{},
			countUsed: make([]int, cfg.Size),
		},
	}
	lb.startGarbageCollector()
	return lb
}
