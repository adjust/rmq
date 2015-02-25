package main

import (
	"github.com/adjust/queue"
	"log"
)

func main() {
	connection := queue.OpenConnection("returner", "localhost", "6379", 2)
	queue := connection.OpenQueue("things")
	returned := queue.ReturnAllRejectedDeliveries()
	log.Printf("queue returner returned %d rejected deliveries", returned)
}
