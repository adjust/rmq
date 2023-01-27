package rmq

import "sync"

type TestBatchConsumer struct {
	mu sync.Mutex
	// Deprecated: use Last() to avoid data races.
	LastBatch Deliveries
	// Deprecated use Consumed() to avoid data races.
	ConsumedCount int64
	AutoFinish    bool

	finish chan int
}

func NewTestBatchConsumer() *TestBatchConsumer {
	return &TestBatchConsumer{
		finish: make(chan int),
	}
}

func (consumer *TestBatchConsumer) Last() Deliveries {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()

	return consumer.LastBatch
}

func (consumer *TestBatchConsumer) Consumed() int64 {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()

	return consumer.ConsumedCount
}

func (consumer *TestBatchConsumer) Consume(batch Deliveries) {
	consumer.mu.Lock()
	consumer.LastBatch = batch
	consumer.ConsumedCount += int64(len(batch))
	consumer.mu.Unlock()

	if consumer.AutoFinish {
		batch.Ack()
	} else {
		<-consumer.finish
		// log.Printf("TestBatchConsumer.Consume() finished")
	}
}

func (consumer *TestBatchConsumer) Finish() {
	// log.Printf("TestBatchConsumer.Finish()")
	consumer.mu.Lock()
	consumer.LastBatch = nil
	consumer.mu.Unlock()

	consumer.finish <- 1
}
