package main

import (
	"fmt"
	"log"
	"time"

	"github.com/adjust/goenv"
	"github.com/adjust/rmq"
)

const (
	numDeliveries = 100000000
	batchSize     = 10000
)

func main() {
	goenv := goenv.NewGoenv("../config.yml", "production", "nil")
	connection := rmq.OpenConnection(rmq.SettingsFromGoenv("producer", goenv))
	things := connection.OpenQueue("things")
	balls := connection.OpenQueue("balls")
	var before time.Time

	for i := 0; i < numDeliveries; i++ {
		delivery := fmt.Sprintf("delivery %d", i)
		things.Publish(delivery)
		if i%batchSize == 0 {
			duration := time.Now().Sub(before)
			before = time.Now()
			perSecond := time.Second / (duration / batchSize)
			log.Printf("produced %d %s %d", i, delivery, perSecond)
			balls.Publish("ball")
		}
	}
}
