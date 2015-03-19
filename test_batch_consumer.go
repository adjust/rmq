package queue

import "log"

type TestBatchConsumer struct {
	LastBatch []Delivery

	finish chan int
}

func NewTestBatchConsumer() *TestBatchConsumer {
	return &TestBatchConsumer{
		finish: make(chan int),
	}
}

func (consumer *TestBatchConsumer) Consume(batch []Delivery) {
	log.Printf("TestBatchConsumer.Consume(%d)", len(batch))
	consumer.LastBatch = batch
	<-consumer.finish
	log.Printf("TestBatchConsumer.Consume() finished")
}

func (consumer *TestBatchConsumer) Finish() {
	log.Printf("TestBatchConsumer.Finish()")
	consumer.LastBatch = nil
	consumer.finish <- 1
}
