package rmq

import (
	"errors"
	"fmt"
	"sync"
)

var errorNotSupported = errors.New("not supported")

type TestConnection struct {
	queues *sync.Map
}

func NewTestConnection() TestConnection {
	return TestConnection{
		queues: &sync.Map{},
	}
}

func (connection TestConnection) OpenQueue(name string) (Queue, error) {
	queue, _ := connection.queues.LoadOrStore(name, NewTestQueue(name))
	return queue.(*TestQueue), nil
}

func (connection TestConnection) CollectStats([]string) (Stats, error)  { panic(errorNotSupported) }
func (connection TestConnection) GetOpenQueues() ([]string, error)      { panic(errorNotSupported) }
func (connection TestConnection) check() error                          { panic(errorNotSupported) }
func (connection TestConnection) getConnections() ([]string, error)     { panic(errorNotSupported) }
func (connection TestConnection) hijackConnection(string) Connection    { panic(errorNotSupported) }
func (connection TestConnection) getConsumingQueues() ([]string, error) { panic(errorNotSupported) }
func (connection TestConnection) close() error                          { panic(errorNotSupported) }
func (connection TestConnection) closeAllQueuesInConnection() error     { panic(errorNotSupported) }
func (connection TestConnection) openQueue(string) Queue                { panic(errorNotSupported) }
func (connection TestConnection) stopHeartbeat() error                  { panic(errorNotSupported) }

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
