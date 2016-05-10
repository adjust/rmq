package main

import (
	"time"

	"github.com/adjust/rmq"
)

func main() {
	connection := rmq.OpenConnection("cleaner", "tcp", "localhost:6379", 2)
	cleaner := rmq.NewCleaner(connection)

	// Every five seconds, clean the connection.
	cleaner.KeepConnectionAlive(5 * time.Second)

}
