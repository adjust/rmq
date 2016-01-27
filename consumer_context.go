package rmq

import "sync"

type ConsumerContext struct {
	stopChan chan int
	wg       *sync.WaitGroup
}

func NewConsumerContext() *ConsumerContext {
	wg := &sync.WaitGroup{}
	wg.Add(1)
	return &ConsumerContext{
		stopChan: make(chan int, 1),
		wg:       wg,
	}
}

func (context *ConsumerContext) StopConsuming() *sync.WaitGroup {
	context.stopChan <- 1
	return context.wg
}
