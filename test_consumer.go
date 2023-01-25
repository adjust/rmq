package rmq

import (
	"sync"
	"time"
)

type TestConsumer struct {
	name          string
	AutoAck       bool
	AutoFinish    bool
	SleepDuration time.Duration

	mu sync.Mutex
	// Deprecated: use Last() to avoid data races.
	LastDelivery Delivery
	// Deprecated: use Deliveries() to avoid data races.
	LastDeliveries []Delivery

	finish chan int
}

func NewTestConsumer(name string) *TestConsumer {
	return &TestConsumer{
		name:       name,
		AutoAck:    true,
		AutoFinish: true,
		finish:     make(chan int),
	}
}

func (consumer *TestConsumer) String() string {
	return consumer.name
}

func (consumer *TestConsumer) Last() Delivery {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()

	return consumer.LastDelivery
}

func (consumer *TestConsumer) Deliveries() []Delivery {
	consumer.mu.Lock()
	defer consumer.mu.Unlock()

	return consumer.LastDeliveries
}

func (consumer *TestConsumer) Consume(delivery Delivery) {
	consumer.mu.Lock()
	consumer.LastDelivery = delivery
	consumer.LastDeliveries = append(consumer.LastDeliveries, delivery)
	consumer.mu.Unlock()

	if consumer.SleepDuration > 0 {
		time.Sleep(consumer.SleepDuration)
	}
	if consumer.AutoAck {
		if err := delivery.Ack(); err != nil {
			panic(err)
		}
	}
	if !consumer.AutoFinish {
		<-consumer.finish
	}
}

func (consumer *TestConsumer) Finish() {
	consumer.finish <- 1
}

func (consumer *TestConsumer) FinishAll() {
	close(consumer.finish)
}
