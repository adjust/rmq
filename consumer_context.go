package rmq

type ConsumerContext struct {
	StopChan chan int
}

func NewConsumerContext() *ConsumerContext {
	return &ConsumerContext{
		StopChan: make(chan int, 1),
	}
}
