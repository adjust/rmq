package main

import (
	"log"

	"github.com/adjust/goenv"
	"github.com/adjust/rmq"
)

func main() {
	goenv := goenv.NewGoenv("../config.yml", "production", "nil")
	connection := rmq.OpenConnection(rmq.SettingsFromGoenv("returner", goenv))
	queue := connection.OpenQueue("things")
	returned := queue.ReturnAllRejected()
	log.Printf("queue returner returned %d rejected deliveries", returned)
}
