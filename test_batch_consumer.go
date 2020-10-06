package rmq

type TestBatchConsumer struct {
	LastBatch     Deliveries
	ConsumedCount int64
	AutoFinish    bool

	finish chan int
}

func NewTestBatchConsumer() *TestBatchConsumer {
	return &TestBatchConsumer{
		finish: make(chan int),
	}
}

func (consumer *TestBatchConsumer) Consume(batch Deliveries) {
	consumer.LastBatch = batch
	consumer.ConsumedCount += int64(len(batch))
	if consumer.AutoFinish {
		batch.Ack()
	} else {
		<-consumer.finish
		// log.Printf("TestBatchConsumer.Consume() finished")
	}
}

func (consumer *TestBatchConsumer) Finish() {
	// log.Printf("TestBatchConsumer.Finish()")
	consumer.LastBatch = nil
	consumer.finish <- 1
}
