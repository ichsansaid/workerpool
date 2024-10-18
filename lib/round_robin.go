package workerpool

type RoundRobinLoadBalancer struct {
	size  int
	index int
}

// Ack implements workerpool.LoadBalancer.
func (r *RoundRobinLoadBalancer) Ack(key int32) {}

// Next implements workerpool.LoadBalancer.
func (r *RoundRobinLoadBalancer) Next(key int32) int {
	if r.size == 0 {
		return -1 // or handle the case when there are no workers
	}
	selectedIndex := r.index
	r.index = (r.index + 1) % r.size
	return selectedIndex
}

func NewRoundRobinLoadBalancer(size int) LoadBalancer {
	return &RoundRobinLoadBalancer{
		size:  size,
		index: 0,
	}
}
