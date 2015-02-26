package main

import (
	"github.com/adjust/queue"
	"time"
)

func main() {
	connection := queue.OpenConnection("cleaner", "tcp", "localhost:6379", 2)
	cleaner := queue.NewCleaner(connection)

	for _ = range time.Tick(time.Second) {
		cleaner.Clean()
	}
}
