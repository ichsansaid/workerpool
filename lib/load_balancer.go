package workerpool

type LoadBalancer interface {
	Next(key int32) int
	Ack(key int32)
}
