package rmq

type Consumer interface {
	Consume(delivery Delivery)
}

type ConsumerFunc func(Delivery)

func (cf ConsumerFunc) Consume(delivery Delivery) {
	cf(delivery)
}
