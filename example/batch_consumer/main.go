package main

import (
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
	batchSize     = 111
	batchTimeout  = time.Second

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

	for _, queueName := range []string{
		"things",
		"foobars",
	} {
		queue, err := connection.OpenQueue(queueName)
		if err != nil {
			panic(err)
		}
		if err := queue.StartConsuming(prefetchLimit, pollDuration); err != nil {
			panic(err)
		}
		if _, err := queue.AddBatchConsumer(queueName, batchSize, batchTimeout, NewBatchConsumer(queueName)); err != nil {
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

type BatchConsumer struct {
	tag string
}

func NewBatchConsumer(tag string) *BatchConsumer {
	return &BatchConsumer{tag: tag}
}

func (consumer *BatchConsumer) Consume(batch rmq.Deliveries) {
	payloads := batch.Payloads()
	debugf("start consume %q", payloads)
	time.Sleep(consumeDuration)

	log.Printf("%s consumed %d: %s", consumer.tag, len(batch), batch[0])
	errors := batch.Ack()
	if len(errors) == 0 {
		debugf("acked %q", payloads)
		return
	}

	for i, err := range errors {
		debugf("failed to ack %q: %q", batch[i].Payload(), err)
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
