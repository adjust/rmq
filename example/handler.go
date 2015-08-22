package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/adjust/goenv"
	"github.com/adjust/rmq"
)

func main() {
	goenv := goenv.NewGoenv("../config.yml", "production", "nil")
	connection := rmq.OpenConnection(rmq.SettingsFromGoenv("handler", goenv))
	http.Handle("/overview", NewHandler(connection))
	http.ListenAndServe(":3333", nil)
}

type Handler struct {
	connection rmq.Connection
}

func NewHandler(connection rmq.Connection) *Handler {
	return &Handler{connection: connection}
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, httpRequest *http.Request) {
	queues := handler.connection.GetOpenQueues()
	stats := handler.connection.CollectStats(queues)
	log.Printf("queue stats\n%s", stats)
	fmt.Fprint(writer, stats.GetHtml("", ""))
}
