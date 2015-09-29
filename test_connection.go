package rmq

import "fmt"

type TestConnection struct {
	queues map[string]*TestQueue
}

func NewTestConnection() TestConnection {
	return TestConnection{
		queues: map[string]*TestQueue{},
	}
}

func (connection TestConnection) OpenQueue(name string) Queue {
	if queue, ok := connection.queues[name]; ok {
		return queue
	}

	queue := NewTestQueue(name)
	connection.queues[name] = queue
	return queue
}

func (connection TestConnection) CollectStats(queueList []string) Stats {
	return Stats{}
}

func (connection TestConnection) GetDeliveries(queueName string) []string {
	queue, ok := connection.queues[queueName]
	if !ok {
		return []string{}
	}

	return queue.LastDeliveries
}

func (connection TestConnection) GetDelivery(queueName string, index int) string {
	queue, ok := connection.queues[queueName]
	if !ok || index < 0 || index >= len(queue.LastDeliveries) {
		return fmt.Sprintf("rmq.TestConnection: delivery not found: %s[%d]", queueName, index)
	}

	return queue.LastDeliveries[index]
}

func (connection TestConnection) Reset() {
	for _, queue := range connection.queues {
		queue.Reset()
	}
}

func (connection TestConnection) GetOpenQueues() []string {
	return []string{}
}
