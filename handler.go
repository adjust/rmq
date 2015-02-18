package queue

import (
	"bytes"
	"fmt"
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
	queueStats := QueueStats{}
	for _, queueName := range handler.connection.GetOpenQueues() {
		queue := handler.connection.openQueue(queueName)
		queueStats[queueName] = NewQueueStat(queue.ReadyCount())
	}

	connectionNames := handler.connection.GetConnections()
	for _, connectionName := range connectionNames {
		connection := handler.connection.hijackConnection(connectionName)
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

	var buffer bytes.Buffer
	buffer.WriteString(`<html><body><table style="font-family:monospace">`)
	buffer.WriteString(fmt.Sprintf(`<tr><td>%s</td><td></td><td>%s</td><td></td><td style="color:lightgrey">%s</td><td></td><td>%s</td><td></td><td>%s</td><td></td></tr>`,
		"queue", "ready", "connection", "unacked", "consumers",
	))

	for queueName, queueStat := range queueStats {
		// log.Printf(" queue:%s ready:%d unacked:%d consumers:%d",
		// 	queueName, queueStat.ReadyCount, queueStat.UnackedCount(), queueStat.ConsumerCount(),
		// )
		buffer.WriteString(fmt.Sprintf(`<tr><td>%s</td><td></td><td>%d</td><td></td><td>%s</td><td></td><td>%d</td><td></td><td>%d</td><td></td></tr>`,
			queueName, queueStat.ReadyCount, "", queueStat.UnackedCount(), queueStat.ConsumerCount(),
		))

		for connectionName, connectionStat := range queueStat.ConnectionStats {
			// log.Printf("  connection:%s unacked:%d consumers:%d",
			// 	connectionName, connectionStat.UnackedCount, len(connectionStat.Consumers),
			// )
			buffer.WriteString(fmt.Sprintf(`<tr style="color:lightgrey"><td>%s</td><td></td><td>%s</td><td></td><td>%s</td><td></td><td>%d</td><td></td><td>%d</td><td></td></tr>`,
				"", "", connectionName, connectionStat.UnackedCount, len(connectionStat.Consumers),
			))
		}
	}

	buffer.WriteString(`</table></body></html>`)
	fmt.Fprint(writer, buffer.String())
}
