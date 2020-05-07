package main

import (
	"log"
	"time"

	"github.com/adjust/rmq/v2"
)

const unackedLimit = 1000

func main() {
	connection, err := rmq.OpenConnection("consumer", "tcp", "localhost:6379", 2, nil)
	if err != nil {
		panic(err)
	}

	queue, err := connection.OpenQueue("things")
	if err != nil {
		panic(err)
	}
	if err := queue.StartConsuming(unackedLimit, 500*time.Millisecond, nil); err != nil {
		panic(err)
	}
	if _, err := queue.AddBatchConsumer("things", 111, NewBatchConsumer("things")); err != nil {
		panic(err)
	}

	queue, err = connection.OpenQueue("balls")
	if err != nil {
		panic(err)
	}
	if err := queue.StartConsuming(unackedLimit, 500*time.Millisecond, nil); err != nil {
		panic(err)
	}
	if _, err := queue.AddBatchConsumer("balls", 111, NewBatchConsumer("balls")); err != nil {
		panic(err)
	}

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
	if errors := batch.Ack(); len(errors) > 0 {
		log.Printf("failed to ack: %s", errors)
	}
}
