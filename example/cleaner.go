package main

import (
	"time"

	"github.com/adjust/goenv"
	"github.com/adjust/rmq"
)

func main() {
	goenv := goenv.NewGoenv("../config.yml", "production", "nil")
	connection := rmq.OpenConnection(rmq.SettingsFromGoenv("cleaner", goenv))
	cleaner := rmq.NewCleaner(connection)

	for _ = range time.Tick(time.Second) {
		cleaner.Clean()
	}
}
