package queue

import (
	"log"
)

type TestConsumer struct {
	LastDelivery   Delivery
	LastDeliveries []Delivery
}

func NewTestConsumer() *TestConsumer {
	return &TestConsumer{}
}

func (consumer *TestConsumer) Consume(delivery Delivery) {
	consumer.LastDelivery = delivery
	consumer.LastDeliveries = append(consumer.LastDeliveries, delivery)
	log.Printf("test consumer consumed delivery %s", delivery)
}
