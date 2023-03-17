package rmq

type BatchConsumer interface {
	Consume(batch Deliveries)
}

type BatchConsumerFunc func(Deliveries)

func (batchConsumerFunc BatchConsumerFunc) Consume(batch Deliveries) {
	batchConsumerFunc(batch)
}
