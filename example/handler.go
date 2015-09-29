package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/adjust/rmq"
)

func main() {
	connection := rmq.OpenConnection("handler", "tcp", "localhost:6379", 2)
	http.Handle("/overview", NewHandler(connection))
	fmt.Printf("Handler listening on http://localhost:3333/overview\n")
	http.ListenAndServe(":3333", nil)
}

type Handler struct {
	connection rmq.Connection
}

func NewHandler(connection rmq.Connection) *Handler {
	return &Handler{connection: connection}
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	layout := request.FormValue("layout")
	refresh := request.FormValue("refresh")

	queues := handler.connection.GetOpenQueues()
	stats := handler.connection.CollectStats(queues)
	log.Printf("queue stats\n%s", stats)
	fmt.Fprint(writer, stats.GetHtml(layout, refresh))
}
