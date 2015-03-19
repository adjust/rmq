package queue

type BatchConsumer interface {
	Consume(batch Deliveries)
}
