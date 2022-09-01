package main

import (
	"log"
	"math"

	"github.com/adjust/rmq/v5"
)

func main() {
	connection, err := rmq.OpenConnection("returner", "tcp", "localhost:6379", 2, nil)
	if err != nil {
		panic(err)
	}

	queue, err := connection.OpenQueue("things")
	if err != nil {
		panic(err)
	}
	returned, err := queue.ReturnRejected(math.MaxInt64)
	if err != nil {
		panic(err)
	}

	log.Printf("queue returner returned %d rejected deliveries", returned)
}
