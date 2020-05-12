package main

import (
	"log"
	"time"

	"github.com/adjust/rmq/v2"
)

const unackedLimit = 1000

func main() {
	connection := rmq.OpenConnection("consumer", "tcp", "localhost:6379", 2)

	queue := connection.OpenQueue("things")
	queue.StartConsuming(unackedLimit, 500*time.Millisecond)
	queue.AddBatchConsumer("things", 111, NewBatchConsumer("things"))

	queue = connection.OpenQueue("balls")
	queue.StartConsuming(unackedLimit, 500*time.Millisecond)
	queue.AddBatchConsumer("balls", 111, NewBatchConsumer("balls"))

	select {}
}

type BatchConsumer struct {
	tag string
}

func NewBatchConsumer(tag string) *BatchConsumer {
	return &BatchConsumer{tag: tag}
}

func (consumer *BatchConsumer) Consume(batch rmq.Deliveries) {
	time.Sleep(time.Millisecond)
	log.Printf("%s consumed %d: %s", consumer.tag, len(batch), batch[0])
	batch.Ack()
}
