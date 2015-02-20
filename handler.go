package queue

import (
	"fmt"
	"log"
	"net/http"
)

type Handler struct {
	connection *Connection
}

func NewHandler(connection *Connection) *Handler {
	return &Handler{
		connection: connection,
	}
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, httpRequest *http.Request) {
	queueStats := CollectStats(handler.connection)

	log.Printf("queue stats %s", queueStats)
	fmt.Fprint(writer, queueStats.GetHtml())
}
