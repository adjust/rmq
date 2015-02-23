package main

import (
	"fmt"
	"log"
	"time"

	"github.com/adjust/queue"
)

const (
	numConsumers = 1
	batchSize    = 5000
)

func main() {
	connection := queue.OpenConnection("consumer", "localhost", "6379", 2)
	queue := connection.OpenQueue("things")
	queue.StartConsuming(100)
	for i := 0; i < numConsumers; i++ {
		name := fmt.Sprintf("consumer %d", i)
		queue.AddConsumer(name, &Consumer{})
	}
	select {}
}

type Consumer struct {
	count  int
	before time.Time
}

func (consumer *Consumer) Consume(delivery queue.Delivery) {
	consumer.count++
	if consumer.count%batchSize == 0 {
		duration := time.Now().Sub(consumer.before)
		consumer.before = time.Now()
		perSecond := time.Second / (duration / batchSize)
		log.Printf("consumed %d %s %d", consumer.count, delivery.Payload(), perSecond)
	}
	delivery.Ack()
}
