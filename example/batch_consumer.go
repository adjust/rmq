package main

import (
	"log"
	"time"

	"github.com/adjust/rmq"
)

const unackedLimit = 1000

func main() {
	connection := rmq.OpenConnection("consumer", "tcp", "localhost:6379", 2)
	queue := connection.OpenQueue("things")
	queue.StartConsuming(unackedLimit, 500*time.Millisecond)
	queue.AddBatchConsumer("batch", 111, NewBatchConsumer())
	select {}
}

type BatchConsumer struct {
}

func NewBatchConsumer() *BatchConsumer {
	return &BatchConsumer{}
}

func (consumer *BatchConsumer) Consume(batch rmq.Deliveries) {
	time.Sleep(time.Millisecond)
	log.Printf("consumed %d", len(batch))
	batch.Ack()
}
