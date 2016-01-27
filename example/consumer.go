package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/adjust/rmq"
)

const (
	unackedLimit = 1000
	numConsumers = 10
	batchSize    = 1000
)

func main() {
	connection := rmq.OpenConnection("consumer", "tcp", "localhost:6379", 2)
	queue := connection.OpenQueue("things")
	queue.StartConsuming(unackedLimit, 500*time.Millisecond)

	contexts := []*rmq.ConsumerContext{}
	for i := 0; i < numConsumers; i++ {
		name := fmt.Sprintf("consumer %d", i)
		_, context := queue.AddConsumer(name, NewConsumer(i))
		contexts = append(contexts, context)
	}

	time.Sleep(time.Second)
	wgs := []*sync.WaitGroup{}
	for i, context := range contexts {
		wg := context.StopConsuming()
		wgs = append(wgs, wg)
		log.Printf("stopped %d", i)
	}
	for i, wg := range wgs {
		wg.Wait()
		log.Printf("waited %d", i)
	}

	time.Sleep(time.Second)
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
	// if consumer.count%batchSize == 0 {
	// 	duration := time.Now().Sub(consumer.before)
	// 	consumer.before = time.Now()
	// 	perSecond := time.Second / (duration / batchSize)
	// 	log.Printf("%s consumed %d %s %d", consumer.name, consumer.count, delivery.Payload(), perSecond)
	// }

	log.Printf("before %s %s", consumer.name, delivery.Payload())
	time.Sleep(10 * time.Millisecond)
	log.Printf("after %s %s", consumer.name, delivery.Payload())

	if consumer.count%batchSize == 0 {
		delivery.Reject()
	} else {
		delivery.Ack()
	}
}
