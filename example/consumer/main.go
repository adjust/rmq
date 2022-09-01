package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adjust/rmq/v5"
)

const (
	prefetchLimit = 1000
	pollDuration  = 100 * time.Millisecond
	numConsumers  = 5

	reportBatchSize = 10000
	consumeDuration = time.Millisecond
	shouldLog       = false
)

func main() {
	errChan := make(chan error, 10)
	go logErrors(errChan)

	connection, err := rmq.OpenConnection("consumer", "tcp", "localhost:6379", 2, errChan)
	if err != nil {
		panic(err)
	}

	queue, err := connection.OpenQueue("things")
	if err != nil {
		panic(err)
	}

	if err := queue.StartConsuming(prefetchLimit, pollDuration); err != nil {
		panic(err)
	}

	for i := 0; i < numConsumers; i++ {
		name := fmt.Sprintf("consumer %d", i)
		if _, err := queue.AddConsumer(name, NewConsumer(i)); err != nil {
			panic(err)
		}
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT)
	defer signal.Stop(signals)

	<-signals // wait for signal
	go func() {
		<-signals // hard exit on second signal (in case shutdown gets stuck)
		os.Exit(1)
	}()

	<-connection.StopAllConsuming() // wait for all Consume() calls to finish
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
	payload := delivery.Payload()
	debugf("start consume %s", payload)
	time.Sleep(consumeDuration)

	consumer.count++
	if consumer.count%reportBatchSize == 0 {
		duration := time.Now().Sub(consumer.before)
		consumer.before = time.Now()
		perSecond := time.Second / (duration / reportBatchSize)
		log.Printf("%s consumed %d %s %d", consumer.name, consumer.count, payload, perSecond)
	}

	if consumer.count%reportBatchSize > 0 {
		if err := delivery.Ack(); err != nil {
			debugf("failed to ack %s: %s", payload, err)
		} else {
			debugf("acked %s", payload)
		}
	} else { // reject one per batch
		if err := delivery.Reject(); err != nil {
			debugf("failed to reject %s: %s", payload, err)
		} else {
			debugf("rejected %s", payload)
		}
	}
}

func logErrors(errChan <-chan error) {
	for err := range errChan {
		switch err := err.(type) {
		case *rmq.HeartbeatError:
			if err.Count == rmq.HeartbeatErrorLimit {
				log.Print("heartbeat error (limit): ", err)
			} else {
				log.Print("heartbeat error: ", err)
			}
		case *rmq.ConsumeError:
			log.Print("consume error: ", err)
		case *rmq.DeliveryError:
			log.Print("delivery error: ", err.Delivery, err)
		default:
			log.Print("other error: ", err)
		}
	}
}

func debugf(format string, args ...interface{}) {
	if shouldLog {
		log.Printf(format, args...)
	}
}
