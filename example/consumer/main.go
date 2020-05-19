package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/adjust/rmq/v2"
)

const (
	unackedLimit    = 1000
	numConsumers    = 5
	batchSize       = 1000
	consumeDuration = time.Millisecond
	shouldLog       = false
)

func main() {
	errChan := make(chan error, 10)
	go func() {
		for err := range errChan {
			switch err := err.(type) {
			case *rmq.ConsumeError:
				log.Print("consume error: ", err)
			case *rmq.HeartbeatError:
				if err.Count == rmq.HeartbeatErrorLimit {
					log.Print("heartbeat error (limit): ", err)
				} else {
					log.Print("heartbeat error: ", err)
				}
			case *rmq.DeliveryError:
				log.Print("delivery error: ", err.Delivery, err)
			default:
				log.Print("other error: ", err)
			}
		}
	}()

	connection, err := rmq.OpenConnection("consumer", "tcp", "localhost:6379", 2, errChan)
	if err != nil {
		panic(err)
	}

	queue, err := connection.OpenQueue("things")
	if err != nil {
		panic(err)
	}

	if err := queue.StartConsuming(unackedLimit, 500*time.Millisecond, errChan); err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	for i := 0; i < numConsumers; i++ {
		name := fmt.Sprintf("consumer %d", i)
		if _, err := queue.AddConsumer(name, NewConsumer(ctx, errChan, i)); err != nil {
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

	c := connection.StopAllConsuming()
	// make sure Ack() and similar calls return with error so that they can be
	// handled and the active Conume() calls can finish
	cancel()
	<-c // wait for all Conume() calls to finish
}

type Consumer struct {
	ctx     context.Context
	errChan chan<- error
	name    string
	count   int
	before  time.Time
}

func NewConsumer(ctx context.Context, errChan chan<- error, tag int) *Consumer {
	return &Consumer{
		ctx:     ctx,
		errChan: errChan,
		name:    fmt.Sprintf("consumer%d", tag),
		count:   0,
		before:  time.Now(),
	}
}

func (consumer *Consumer) Consume(delivery rmq.Delivery) {
	payload := delivery.Payload()
	debugf("start consume %s", payload)
	time.Sleep(consumeDuration)

	consumer.count++
	if consumer.count%batchSize == 0 {
		duration := time.Now().Sub(consumer.before)
		consumer.before = time.Now()
		perSecond := time.Second / (duration / batchSize)
		log.Printf("%s consumed %d %s %d", consumer.name, consumer.count, payload, perSecond)
	}

	if consumer.count%batchSize > 0 {
		if err := delivery.Ack(consumer.ctx, consumer.errChan); err != nil {
			debugf("failed to ack %s: %s", payload, err)
		} else {
			debugf("acked %s", payload)
		}
	} else { // reject one per batch
		if err := delivery.Reject(consumer.ctx, consumer.errChan); err != nil {
			debugf("failed to reject %s: %s", payload, err)
		} else {
			debugf("rejected %s", payload)
		}
	}
}

func debugf(format string, args ...interface{}) {
	if shouldLog {
		log.Printf(format, args...)
	}
}
