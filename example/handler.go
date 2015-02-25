package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/adjust/queue"
)

func main() {
	connection := queue.OpenConnection("producer", "localhost", "6379", 2)
	http.Handle("/overview", NewHandler(connection))
	http.ListenAndServe(":3333", nil)
}

type Handler struct {
	connection *queue.Connection
}

func NewHandler(connection *queue.Connection) *Handler {
	return &Handler{connection: connection}
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, httpRequest *http.Request) {
	stats := queue.CollectStats(handler.connection)
	log.Printf("queue stats\n%s", stats)
	fmt.Fprint(writer, stats.GetHtml())
}
