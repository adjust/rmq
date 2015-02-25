package queue

import (
	"bytes"
	"fmt"
	"sort"
)

type ConnectionStat struct {
	Active       bool
	UnackedCount int
	Consumers    []string
}

func (stat ConnectionStat) String() string {
	return fmt.Sprintf("[unacked:%d consumers:%d]",
		stat.UnackedCount,
		len(stat.Consumers),
	)
}

type ConnectionStats map[string]ConnectionStat

type QueueStat struct {
	ReadyCount      int
	RejectedCount   int
	ConnectionStats ConnectionStats
}

func NewQueueStat(readyCount, rejectedCount int) QueueStat {
	return QueueStat{
		ReadyCount:      readyCount,
		RejectedCount:   rejectedCount,
		ConnectionStats: ConnectionStats{},
	}
}

func (stat QueueStat) String() string {
	return fmt.Sprintf("[ready:%d rejected:%d conn:%s",
		stat.ReadyCount,
		stat.RejectedCount,
		stat.ConnectionStats,
	)
}

func (stat QueueStat) UnackedCount() int {
	unacked := 0
	for _, connectionStat := range stat.ConnectionStats {
		unacked += connectionStat.UnackedCount
	}
	return unacked
}

func (stat QueueStat) ConsumerCount() int {
	consumer := 0
	for _, connectionStat := range stat.ConnectionStats {
		consumer += len(connectionStat.Consumers)
	}
	return consumer
}

type Stats struct {
	queueStats       map[string]QueueStat
	otherConnections map[string]bool // non consuming connections, active or not
}

func NewStats() Stats {
	return Stats{
		queueStats:       map[string]QueueStat{},
		otherConnections: map[string]bool{},
	}
}

func CollectStats(mainConnection *Connection) Stats {
	stats := NewStats()
	for _, queueName := range mainConnection.GetOpenQueues() {
		queue := mainConnection.openQueue(queueName)
		stats.queueStats[queueName] = NewQueueStat(queue.ReadyCount(), queue.RejectedCount())
	}

	connectionNames := mainConnection.GetConnections()
	for _, connectionName := range connectionNames {
		connection := mainConnection.hijackConnection(connectionName)
		connectionActive := connection.Check()

		queueNames := connection.GetConsumingQueues()
		if len(queueNames) == 0 {
			stats.otherConnections[connectionName] = connectionActive
			continue
		}

		for _, queueName := range queueNames {
			queue := connection.openQueue(queueName)
			consumers := queue.GetConsumers()

			stats.queueStats[queueName].ConnectionStats[connectionName] = ConnectionStat{
				Active:       connectionActive,
				UnackedCount: queue.UnackedCount(),
				Consumers:    consumers,
			}
		}
	}

	return stats
}

func (stats Stats) String() string {
	var buffer bytes.Buffer

	for queueName, queueStat := range stats.queueStats {
		buffer.WriteString(fmt.Sprintf("    queue:%s ready:%d rejected:%d unacked:%d consumers:%d\n",
			queueName, queueStat.ReadyCount, queueStat.RejectedCount, queueStat.UnackedCount(), queueStat.ConsumerCount(),
		))

		for connectionName, connectionStat := range queueStat.ConnectionStats {
			buffer.WriteString(fmt.Sprintf("        connection:%s unacked:%d consumers:%d active:%t\n",
				connectionName, connectionStat.UnackedCount, len(connectionStat.Consumers), connectionStat.Active,
			))
		}
	}

	for connectionName, active := range stats.otherConnections {
		buffer.WriteString(fmt.Sprintf("    connection:%s active:%t\n",
			connectionName, active,
		))
	}

	return buffer.String()
}

func (stats Stats) GetHtml() string {
	var buffer bytes.Buffer
	buffer.WriteString(`<html><body><table style="font-family:monospace">`)
	buffer.WriteString(`<tr><td>` +
		`queue</td><td></td><td>` +
		`ready</td><td></td><td>` +
		`rejected</td><td></td><td>` +
		`</td><td></td><td style="color:lightgrey">` +
		`connection</td><td></td><td>` +
		`unacked</td><td></td><td>` +
		`consumers</td><td></td></tr>`,
	)

	for _, queueName := range stats.sortedQueueNames() {
		queueStat := stats.queueStats[queueName]
		buffer.WriteString(fmt.Sprintf(`<tr><td>`+
			`%s</td><td></td><td>`+
			`%d</td><td></td><td>`+
			`%d</td><td></td><td>`+
			`%s</td><td></td><td>`+
			`%s</td><td></td><td>`+
			`%d</td><td></td><td>`+
			`%d</td><td></td></tr>`,
			queueName, queueStat.ReadyCount, queueStat.RejectedCount, "", "", queueStat.UnackedCount(), queueStat.ConsumerCount(),
		))

		for _, connectionName := range queueStat.ConnectionStats.sortedNames() {
			connectionStat := queueStat.ConnectionStats[connectionName]
			buffer.WriteString(fmt.Sprintf(`<tr style="color:lightgrey"><td>`+
				`%s</td><td></td><td>`+
				`%s</td><td></td><td>`+
				`%s</td><td></td><td>`+
				`%s</td><td></td><td>`+
				`%s</td><td></td><td>`+
				`%d</td><td></td><td>`+
				`%d</td><td></td></tr>`,
				"", "", "", ActiveSign(connectionStat.Active), connectionName, connectionStat.UnackedCount, len(connectionStat.Consumers),
			))
		}
	}

	buffer.WriteString(`<tr><td>-----</td></tr>`)
	for _, connectionName := range stats.sortedConnectionNames() {
		active := stats.otherConnections[connectionName]
		buffer.WriteString(fmt.Sprintf(`<tr style="color:lightgrey"><td>`+
			`%s</td><td></td><td>`+
			`%s</td><td></td><td>`+
			`%s</td><td></td><td>`+
			`%s</td><td></td><td>`+
			`%s</td><td></td><td>`+
			`%s</td><td></td><td>`+
			`%s</td><td></td></tr>`,
			"", "", "", ActiveSign(active), connectionName, "", "",
		))
	}

	buffer.WriteString(`</table></body></html>`)
	return buffer.String()
}

func (stats ConnectionStats) sortedNames() []string {
	var keys []string
	for key := range stats {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (stats Stats) sortedQueueNames() []string {
	var keys []string
	for key := range stats.queueStats {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func (stats Stats) sortedConnectionNames() []string {
	var keys []string
	for key := range stats.otherConnections {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func ActiveSign(active bool) string {
	if active {
		return "✓"
	}
	return "✗"
}
