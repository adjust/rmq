package main

import (
	"github.com/adjust/queue"
)

func main() {
	connection := queue.OpenConnection("returner", "localhost", "6379", 2)
	queue := connection.OpenQueue("things")
	queue.ReturnAllRejectedDeliveries()
}
