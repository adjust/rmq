package main

import (
	"fmt"
	"log"
	"time"

	"github.com/adjust/queue"
)

const (
	numDeliveries = 100000000
	batchSize     = 5000
)

func main() {
	connection := queue.OpenConnection("producer", "localhost", "6379", 2)
	queue := connection.OpenQueue("things")
	var before time.Time

	for i := 0; i < numDeliveries; i++ {
		delivery := fmt.Sprintf("delivery %d", i)
		queue.Publish(delivery)
		if i%batchSize == 0 {
			duration := time.Now().Sub(before)
			before = time.Now()
			perSecond := time.Second / (duration / batchSize)
			log.Printf("produced %d %s %d", i, delivery, perSecond)
		}
	}
}
