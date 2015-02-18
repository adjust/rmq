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

	for queueName, queueStat := range queueStats {
		log.Printf(" queue:%s ready:%d unacked:%d consumers:%d",
			queueName,
			queueStat.ReadyCount,
			queueStat.UnackedCount(),
			queueStat.ConsumerCount(),
		)

		for connectionName, connectionStat := range queueStat.ConnectionStats {
			log.Printf("  connection:%s unacked:%d consumers:%d",
				connectionName,
				connectionStat.UnackedCount,
				len(connectionStat.Consumers),
			)
		}
	}
}
