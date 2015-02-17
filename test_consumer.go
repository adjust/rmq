package queue

type TestConsumer struct {
	AutoAck        bool
	LastDelivery   Delivery
	LastDeliveries []Delivery
}

func NewTestConsumer() *TestConsumer {
	return &TestConsumer{}
}

func (consumer *TestConsumer) Consume(delivery Delivery) {
	consumer.LastDelivery = delivery
	consumer.LastDeliveries = append(consumer.LastDeliveries, delivery)

	if consumer.AutoAck {
		delivery.Ack()
	}
}
