package queue

import "fmt"

type ConnectionStat struct {
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
	ConnectionStats ConnectionStats
}

func NewQueueStat(readyCount int) QueueStat {
	return QueueStat{
		ReadyCount:      readyCount,
		ConnectionStats: ConnectionStats{},
	}
}

func (stat QueueStat) String() string {
	return fmt.Sprintf("[ready:%d conn:%s",
		stat.ReadyCount,
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

type QueueStats map[string]QueueStat

func CollectStats(mainConnection *Connection) QueueStats {
	queueStats := QueueStats{}
	for _, queueName := range mainConnection.GetOpenQueues() {
		queue := mainConnection.openQueue(queueName)
		queueStats[queueName] = NewQueueStat(queue.ReadyCount())
	}

	connectionNames := mainConnection.GetConnections()
	for _, connectionName := range connectionNames {
		connection := mainConnection.hijackConnection(connectionName)
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

	return queueStats
}
