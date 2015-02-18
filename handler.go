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

	queueStats := QueueStats{}
	for _, queueName := range handler.connection.GetOpenQueues() {
		queue := handler.connection.openQueue(queueName)
		queueStats[queueName] = NewQueueStat(queue.ReadyCount())
	}

	connectionNames := handler.connection.GetConnections()
	for _, connectionName := range connectionNames {
		connection := handler.connection.openNamedConnection(connectionName)
		queueNames := connection.GetConsumingQueues()

		for _, queueName := range queueNames {
			queue := connection.openQueue(queueName)
			consumers := queue.GetConsumers()

			queueStats[queueName].ConnectionStats[connectionName] = ConnectionStat{
				UnackedCount: queue.UnackedCount(),
				Consumers:    consumers,
			}
		}
	}

	log.Printf("queues %s", queueStats)
}
