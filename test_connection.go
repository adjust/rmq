package rmq

import (
	"fmt"
	"sync"
)

type TestConnection struct {
	queues *sync.Map
}

func NewTestConnection() TestConnection {
	return TestConnection{
		queues: &sync.Map{},
	}
}

func (connection TestConnection) OpenQueue(name string) Queue {
	queue, _ := connection.queues.LoadOrStore(name, NewTestQueue(name))
	return queue.(*TestQueue)
}

func (connection TestConnection) CollectStats(queueList []string) Stats {
	return Stats{}
}

func (connection TestConnection) GetDeliveries(queueName string) []string {
	queue, ok := connection.queues.Load(queueName)
	if !ok {
		return []string{}
	}

	return queue.(*TestQueue).LastDeliveries
}

func (connection TestConnection) GetDelivery(queueName string, index int) string {
	queue, ok := connection.queues.Load(queueName)
	if !ok || index < 0 || index >= len(queue.(*TestQueue).LastDeliveries) {
		return fmt.Sprintf("rmq.TestConnection: delivery not found: %s[%d]", queueName, index)
	}

	return queue.(*TestQueue).LastDeliveries[index]
}

func (connection TestConnection) Reset() {
	connection.queues.Range(func(_, v interface{}) bool {
		v.(*TestQueue).Reset()
		return true
	})
}

func (connection TestConnection) GetOpenQueues() []string {
	return []string{}
}
