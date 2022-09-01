package main

import (
	"log"

	"github.com/adjust/rmq/v5"
)

func main() {
	connection, err := rmq.OpenConnection("cleaner", "tcp", "localhost:6379", 2, nil)
	if err != nil {
		panic(err)
	}

	queue, err := connection.OpenQueue("things")
	if err != nil {
		panic(err)
	}
	count, err := queue.PurgeReady()
	if err != nil {
		log.Printf("failed to purge: %s", err)
		return
	}

	log.Printf("purged %d", count)
}
