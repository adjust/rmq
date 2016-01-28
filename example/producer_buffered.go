package main

import (
	"fmt"
	"log"
	"time"

	"github.com/adjust/rmq"
)

const numProducers = 3

func main() {
	connection := rmq.OpenConnection("producer", "tcp", "localhost:6379", 2)
	things := connection.OpenQueue("things")

	// start publishing infinitely
	for p := 0; p < numProducers; p++ {
		go func(p int) {
			batchSize := 0
			before := time.Now()
			nextLog := time.Now().Add(100 * time.Millisecond)

			for d := 0; true; d++ {
				delivery := fmt.Sprintf("prod%ddel%d", p, d)
				things.Publish(delivery)
				batchSize++

				// log stats
				if time.Now().After(nextLog) {
					duration := time.Now().Sub(before)
					perSecond := time.Second / (duration / time.Duration(batchSize))
					log.Printf("producer %d produced %d %s %d", p, d, delivery, perSecond)

					before = time.Now()
					batchSize = 0
					nextLog = time.Now().Add(100 * time.Millisecond)
				}
			}
		}(p)
	}

	// schedule publish buffer size changes
	time.Sleep(time.Second)
	// for _, bufferSize := range []int{0, 1, 10, 10, 0, 100, 1000, 10000, 0, 100000, 0} {
	for _, bufferSize := range []int{10, 0, 100, 10, 0} {
		connection.SetPublishBufferSize(bufferSize, 100*time.Millisecond)
		log.Printf("=== %d ===", bufferSize)
		time.Sleep(time.Second)
	}
}
