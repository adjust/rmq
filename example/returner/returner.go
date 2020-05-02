package main

import (
	"github.com/adjust/rmq"
	"log"
)

func main() {
	connection := rmq.OpenConnection("returner", "tcp", "localhost:6379", 2)
	queue := connection.OpenQueue("things")
	returned := queue.ReturnAllRejected()
	log.Printf("queue returner returned %d rejected deliveries", returned)
}
