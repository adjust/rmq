package queue

import "fmt"

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

type QueueStats map[string]QueueStat

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
