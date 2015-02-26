package main

import (
	"time"

	"github.com/adjust/goenv"
	"github.com/adjust/queue"
)

func main() {
	goenv := goenv.NewGoenv("../config.yml", "production", "nil")
	connection := queue.OpenConnection(queue.SettingsFromGoenv("cleaner", goenv))
	cleaner := queue.NewCleaner(connection)

	for _ = range time.Tick(time.Second) {
		cleaner.Clean()
	}
}
