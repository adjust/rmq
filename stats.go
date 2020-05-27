package rmq

import (
	"bytes"
	"fmt"
	"sort"
)

type ConnectionStat struct {
	active       bool
	unackedCount int64
	consumers    []string
}

func (stat ConnectionStat) String() string {
	return fmt.Sprintf("[unacked:%d consumers:%d]",
		stat.unackedCount,
		len(stat.consumers),
	)
}

type ConnectionStats map[string]ConnectionStat

type QueueStat struct {
	ReadyCount      int64 `json:"ready"`
	RejectedCount   int64 `json:"rejected"`
	connectionStats ConnectionStats
}

func NewQueueStat(readyCount, rejectedCount int64) QueueStat {
	return QueueStat{
		ReadyCount:      readyCount,
		RejectedCount:   rejectedCount,
		connectionStats: ConnectionStats{},
	}
}

func (stat QueueStat) String() string {
	return fmt.Sprintf("[ready:%d rejected:%d conn:%s",
		stat.ReadyCount,
		stat.RejectedCount,
		stat.connectionStats,
	)
}

func (stat QueueStat) UnackedCount() int64 {
	unacked := int64(0)
	for _, connectionStat := range stat.connectionStats {
		unacked += connectionStat.unackedCount
	}
	return unacked
}

func (stat QueueStat) ConsumerCount() int64 {
	consumer := int64(0)
	for _, connectionStat := range stat.connectionStats {
		consumer += int64(len(connectionStat.consumers))
	}
	return consumer
}

func (stat QueueStat) ConnectionCount() int64 {
	return int64(len(stat.connectionStats))
}

type QueueStats map[string]QueueStat

type Stats struct {
	QueueStats       QueueStats      `json:"queues"`
	otherConnections map[string]bool // non consuming connections, active or not
}

func NewStats() Stats {
	return Stats{
		QueueStats:       QueueStats{},
		otherConnections: map[string]bool{},
	}
}

func CollectStats(queueList []string, mainConnection Connection) (Stats, error) {
	stats := NewStats()
	for _, queueName := range queueList {
		queue := mainConnection.openQueue(queueName)
		readyCount, err := queue.readyCount()
		if err != nil {
			return stats, err
		}
		rejectedCount, err := queue.rejectedCount()
		if err != nil {
			return stats, err
		}
		stats.QueueStats[queueName] = NewQueueStat(readyCount, rejectedCount)
	}

	connectionNames, err := mainConnection.getConnections()
	if err != nil {
		return stats, err
	}

	for _, connectionName := range connectionNames {
		hijackedConnection := mainConnection.hijackConnection(connectionName)

		var connectionActive bool
		switch err := hijackedConnection.checkHeartbeat(); err {
		case nil:
			connectionActive = true
		case ErrorNotFound:
			connectionActive = false
		default:
			return stats, err
		}

		queueNames, err := hijackedConnection.getConsumingQueues()
		if err != nil {
			return stats, err
		}
		if len(queueNames) == 0 {
			stats.otherConnections[connectionName] = connectionActive
			continue
		}

		for _, queueName := range queueNames {
			queue := hijackedConnection.openQueue(queueName)
			consumers, err := queue.getConsumers()
			if err != nil {
				return stats, err
			}
			openQueueStat, ok := stats.QueueStats[queueName]
			if !ok {
				continue
			}
			unackedCount, err := queue.unackedCount()
			if err != nil {
				return stats, err
			}
			openQueueStat.connectionStats[connectionName] = ConnectionStat{
				active:       connectionActive,
				unackedCount: unackedCount,
				consumers:    consumers,
			}
		}
	}

	return stats, nil
}

func (stats Stats) String() string {
	var buffer bytes.Buffer

	for queueName, queueStat := range stats.QueueStats {
		buffer.WriteString(fmt.Sprintf("    queue:%s ready:%d rejected:%d unacked:%d consumers:%d\n",
			queueName, queueStat.ReadyCount, queueStat.RejectedCount, queueStat.UnackedCount(), queueStat.ConsumerCount(),
		))

		for connectionName, connectionStat := range queueStat.connectionStats {
			buffer.WriteString(fmt.Sprintf("        connection:%s unacked:%d consumers:%d active:%t\n",
				connectionName, connectionStat.unackedCount, len(connectionStat.consumers), connectionStat.active,
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

func (stats Stats) GetHtml(layout, refresh string) string {
	buffer := bytes.NewBufferString("<html>")

	if refresh != "" {
		buffer.WriteString(fmt.Sprintf(`<head><meta http-equiv="refresh" content="%s">`, refresh))
	}

	buffer.WriteString(`<body><table style="font-family:monospace">`)
	buffer.WriteString(`<tr><td>` +
		`queue</td><td></td><td>` +
		`ready</td><td></td><td>` +
		`rejected</td><td></td><td>` +
		`</td><td></td><td>` +
		`connections</td><td></td><td>` +
		`unacked</td><td></td><td>` +
		`consumers</td><td></td></tr>`,
	)

	for _, queueName := range stats.sortedQueueNames() {
		queueStat := stats.QueueStats[queueName]
		connectionNames := queueStat.connectionStats.sortedNames()
		buffer.WriteString(fmt.Sprintf(`<tr><td>`+
			`%s</td><td></td><td>`+
			`%d</td><td></td><td>`+
			`%d</td><td></td><td>`+
			`%s</td><td></td><td>`+
			`%d</td><td></td><td>`+
			`%d</td><td></td><td>`+
			`%d</td><td></td></tr>`,
			queueName, queueStat.ReadyCount, queueStat.RejectedCount, "", len(connectionNames), queueStat.UnackedCount(), queueStat.ConsumerCount(),
		))

		if layout != "condensed" {
			for _, connectionName := range connectionNames {
				connectionStat := queueStat.connectionStats[connectionName]
				buffer.WriteString(fmt.Sprintf(`<tr style="color:lightgrey"><td>`+
					`%s</td><td></td><td>`+
					`%s</td><td></td><td>`+
					`%s</td><td></td><td>`+
					`%s</td><td></td><td>`+
					`%s</td><td></td><td>`+
					`%d</td><td></td><td>`+
					`%d</td><td></td></tr>`,
					"", "", "", ActiveSign(connectionStat.active), connectionName, connectionStat.unackedCount, len(connectionStat.consumers),
				))
			}
		}
	}

	if layout != "condensed" {
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
	for key := range stats.QueueStats {
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
