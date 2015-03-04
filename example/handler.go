package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/adjust/goenv"
	"github.com/adjust/queue"
)

func main() {
	goenv := goenv.NewGoenv("../config.yml", "production", "nil")
	connection := queue.OpenConnection(queue.SettingsFromGoenv("handler", goenv))
	http.Handle("/overview", NewHandler(connection))
	http.ListenAndServe(":3333", nil)
}

type Handler struct {
	connection queue.Connection
}

func NewHandler(connection queue.Connection) *Handler {
	return &Handler{connection: connection}
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, httpRequest *http.Request) {
	stats := handler.connection.CollectStats()
	log.Printf("queue stats\n%s", stats)
	fmt.Fprint(writer, stats.GetHtml())
}
