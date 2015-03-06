package main

import (
	"github.com/adjust/goenv"
	"github.com/adjust/queue"
)

func main() {
	goenv := goenv.NewGoenv("../config.yml", "production", "nil")
	connection := queue.OpenConnection(queue.SettingsFromGoenv("cleaner", goenv))
	queue := connection.OpenQueue("things")
	queue.PurgeReady()
}
