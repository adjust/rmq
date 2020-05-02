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

func (connection TestConnection) CollectStats(queueList []string) (Stats, error) {
	return Stats{}, nil
}

func (connection TestConnection) GetOpenQueues() ([]string, error) {
	return []string{}, nil
}

func (connection TestConnection) check() (bool, error) {
	return true, nil
}
func (connection TestConnection) getConnections() ([]string, error) {
	return nil, nil
}
func (connection TestConnection) hijackConnection(name string) Connection {
	return nil
}
func (connection TestConnection) getConsumingQueues() ([]string, error) {
	return nil, nil
}
func (connection TestConnection) close() error {
	return nil
}
func (connection TestConnection) closeAllQueuesInConnection() error {
	return nil
}
func (connection TestConnection) openQueue(name string) Queue {
	return nil
}
func (connection TestConnection) stopHeartbeat() error {
	return nil
}

// test helpers for test inspection and similar

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
