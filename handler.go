package queue

import (
	"bytes"
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

	var htmlBuffer, logBuffer bytes.Buffer
	htmlBuffer.WriteString(`<html><body><table style="font-family:monospace">`)
	htmlBuffer.WriteString(fmt.Sprintf(`<tr><td>%s</td><td></td><td>%s</td><td></td><td style="color:lightgrey">%s</td><td></td><td>%s</td><td></td><td>%s</td><td></td></tr>`,
		"queue", "ready", "connection", "unacked", "consumers",
	))

	for queueName, queueStat := range queueStats {
		logBuffer.WriteString(fmt.Sprintf("    queue:%s ready:%d unacked:%d consumers:%d\n",
			queueName, queueStat.ReadyCount, queueStat.UnackedCount(), queueStat.ConsumerCount(),
		))
		htmlBuffer.WriteString(fmt.Sprintf(`<tr><td>%s</td><td></td><td>%d</td><td></td><td>%s</td><td></td><td>%d</td><td></td><td>%d</td><td></td></tr>`,
			queueName, queueStat.ReadyCount, "", queueStat.UnackedCount(), queueStat.ConsumerCount(),
		))

		for connectionName, connectionStat := range queueStat.ConnectionStats {
			logBuffer.WriteString(fmt.Sprintf("        connection:%s unacked:%d consumers:%d\n",
				connectionName, connectionStat.UnackedCount, len(connectionStat.Consumers),
			))
			htmlBuffer.WriteString(fmt.Sprintf(`<tr style="color:lightgrey"><td>%s</td><td></td><td>%s</td><td></td><td>%s</td><td></td><td>%d</td><td></td><td>%d</td><td></td></tr>`,
				"", "", connectionName, connectionStat.UnackedCount, len(connectionStat.Consumers),
			))
		}
	}

	log.Printf("queue handler\n%s", logBuffer.String())
	htmlBuffer.WriteString(`</table></body></html>`)
	fmt.Fprint(writer, htmlBuffer.String())
}
