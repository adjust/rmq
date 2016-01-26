package main

import (
	"fmt"
	"log"
	"time"

	"github.com/adjust/rmq"
)

func main() {
	connection := rmq.OpenConnection("producer", "tcp", "localhost:6379", 2)
	things := connection.OpenQueue("things")

	// start publishing infinitely
	go func() {
		batchSize := 0
		before := time.Now()
		nextLog := time.Now().Add(100 * time.Millisecond)

		for i := 0; true; i++ {
			delivery := fmt.Sprintf("delivery%d", i)
			things.Publish(delivery)
			batchSize++

			// log stats
			if time.Now().After(nextLog) {
				duration := time.Now().Sub(before)
				perSecond := time.Second / (duration / time.Duration(batchSize))
				log.Printf("produced %d %s %d", i, delivery, perSecond)

				before = time.Now()
				batchSize = 0
				nextLog = time.Now().Add(100 * time.Millisecond)
			}
		}
	}()

	// schedule publish buffer size changes
	time.Sleep(time.Second)
	for _, bufferSize := range []int{0, 1, 10, 10, 0, 100, 1000, 10000, 0, 100000, 0} {
		connection.SetPublishBufferSize(bufferSize, 100*time.Millisecond)
		log.Printf("=== %d ===", bufferSize)
		time.Sleep(time.Second)
	}
}
