package main

import (
	"github.com/adjust/rmq"
	"time"

)

func main() {
	connection := rmq.OpenConnection("cleaner", "tcp", "localhost:6379", 2)
	cleaner := rmq.NewCleaner(connection)

	for _ = range time.Tick(time.Second) {
		cleaner.Clean()
	}
}
