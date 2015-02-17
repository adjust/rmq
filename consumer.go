package queue

type Consumer interface {
	Consume(delivery Delivery)
}
