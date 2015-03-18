package queue

type BatchConsumer interface {
	Consume(batch []Delivery)
}
