package rmq

type TestBatchConsumer struct {
	LastBatch Deliveries

	finish chan int
}

func NewTestBatchConsumer() *TestBatchConsumer {
	return &TestBatchConsumer{
		finish: make(chan int),
	}
}

func (consumer *TestBatchConsumer) Consume(batch Deliveries) {
	// log.Printf("TestBatchConsumer.Consume(%d)", len(batch))
	consumer.LastBatch = batch
	<-consumer.finish
	// log.Printf("TestBatchConsumer.Consume() finished")
}

func (consumer *TestBatchConsumer) Finish() {
	// log.Printf("TestBatchConsumer.Finish()")
	consumer.LastBatch = nil
	consumer.finish <- 1
}
