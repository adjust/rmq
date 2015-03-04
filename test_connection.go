package queue

import "fmt"

type TestConnection struct {
	queues map[string]*TestQueue
}

func NewTestConnection() TestConnection {
	return TestConnection{
		queues: map[string]*TestQueue{},
	}
}

func (connection TestConnection) OpenQueue(name string) PublishQueue {
	queue := NewTestQueue()
	connection.queues[name] = queue
	return queue
}

func (connection TestConnection) GetDelivery(queueName string, index int) string {
	queue, ok := connection.queues[queueName]
	if !ok || index < 0 || index >= len(queue.LastDeliveries) {
		return fmt.Sprintf("queue.TestConnection: delivery not found: %s[%d]", queueName, index)
	}

	return queue.LastDeliveries[index]
}

func (connection TestConnection) Reset() {
	for _, queue := range connection.queues {
		queue.Reset()
	}
}
