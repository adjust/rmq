package main

import (
	"github.com/adjust/goenv"
	"github.com/adjust/rmq"
)

func main() {
	goenv := goenv.NewGoenv("../config.yml", "production", "nil")
	connection := rmq.OpenConnection(rmq.SettingsFromGoenv("cleaner", goenv))
	queue := connection.OpenQueue("things")
	queue.PurgeReady()
}
