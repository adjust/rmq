package main

import (
	"github.com/adjust/goenv"
	"github.com/adjust/queue"
	"log"
)

func main() {
	goenv := goenv.NewGoenv("../config.yml", "production", "nil")
	connection := queue.OpenConnection(queue.SettingsFromGoenv("returner", goenv))
	queue := connection.OpenQueue("things")
	returned := queue.ReturnAllRejected()
	log.Printf("queue returner returned %d rejected deliveries", returned)
}
