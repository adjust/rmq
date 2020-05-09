package main

import (
	"fmt"
	"log"
	"time"

	"github.com/adjust/rmq/v2"
)

const (
	unackedLimit = 1000
	numConsumers = 10
	batchSize    = 1000
)

func main() {
	errors := make(chan error, 10)
	go func() {
		for err := range errors {
			switch err := err.(type) {
			case *rmq.ConsumeError:
				log.Print("consume error: ", err)
			case *rmq.HeartbeatError:
				if err.Count == rmq.HeartbeatErrorLimit {
					log.Print("heartbeat error (limit): ", err)
				} else {
					log.Print("heartbeat error: ", err)
				}
			default:
				log.Print("other error: ", err)
			}
		}
	}()

	connection, err := rmq.OpenConnection("consumer", "tcp", "localhost:6379", 2, errors)
	if err != nil {
		panic(err)
	}

	queue, err := connection.OpenQueue("things")
	if err != nil {
		panic(err)
	}

	if err := queue.StartConsuming(unackedLimit, 500*time.Millisecond, errors); err != nil {
		panic(err)
	}

	for i := 0; i < numConsumers; i++ {
		name := fmt.Sprintf("consumer %d", i)
		if _, err := queue.AddConsumer(name, NewConsumer(i)); err != nil {
			panic(err)
		}
	}
	select {}
}

type Consumer struct {
	name   string
	count  int
	before time.Time
}

func NewConsumer(tag int) *Consumer {
	return &Consumer{
		name:   fmt.Sprintf("consumer%d", tag),
		count:  0,
		before: time.Now(),
	}
}

func (consumer *Consumer) Consume(delivery rmq.Delivery) {
	consumer.count++
	if consumer.count%batchSize == 0 {
		duration := time.Now().Sub(consumer.before)
		consumer.before = time.Now()
		perSecond := time.Second / (duration / batchSize)
		log.Printf("%s consumed %d %s %d", consumer.name, consumer.count, delivery.Payload(), perSecond)
	}
	time.Sleep(time.Millisecond)
	if consumer.count%batchSize == 0 {
		if err := delivery.Reject(); err != nil {
			log.Printf("failed to reject: %s", err)
		}
	} else {
		if err := delivery.Ack(); err != nil {
			log.Printf("failed to ack: %s", err)
		}
	}
}
