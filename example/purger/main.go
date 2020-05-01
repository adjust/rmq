package main

import (
	"github.com/adjust/rmq/v2"
	"log"
)

func main() {
	connection, err := rmq.OpenConnection("cleaner", "tcp", "localhost:6379", 2)
	if err != nil {
		panic(err)
	}

	queue := connection.OpenQueue("things")
	count, err := queue.PurgeReady()
	if err != nil {
		log.Printf("failed to purge: %s", err)
		return
	}

	log.Printf("purged %d", count)
}
