package queue

type TestBatchConsumer struct {
}

func NewTestBatchConsumer() TestBatchConsumer {
	return TestBatchConsumer{}
}

func (consumer TestBatchConsumer) Consume(batch []Delivery) {
}
