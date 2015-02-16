package queue

type TestConsumer struct {
}

func NewTestConsumer() *TestConsumer {
	return &TestConsumer{}
}

func (consumer *TestConsumer) Consume(delivery Delivery) {
}
