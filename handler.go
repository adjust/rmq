package queue

import (
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
	log.Printf("\n\n")

	connectionNames := handler.connection.GetConnections()
	queues := handler.connection.GetOpenQueues()
	log.Printf("queues %s", queues)

	for _, connectionName := range connectionNames {
		connection := handler.connection.openNamedConnection(connectionName)
		log.Printf("connection %s %t", connection, connection.Check())
		queueNames := connection.GetConsumingQueues()

		for _, queueName := range queueNames {
			queue := connection.openQueue(queueName)
			consumers := queue.GetConsumers()

			log.Printf(" queue %s %d %d %s", queue, queue.ReadyCount(), queue.UnackedCount(), consumers)
		}
	}
}
