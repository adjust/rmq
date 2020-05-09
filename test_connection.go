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

func (TestConnection) CollectStats([]string) (Stats, error)  { panic(errorNotSupported) }
func (TestConnection) GetOpenQueues() ([]string, error)      { panic(errorNotSupported) }
func (TestConnection) StopAllConsuming() <-chan struct{}     { panic(errorNotSupported) }
func (TestConnection) checkHeartbeat() error                 { panic(errorNotSupported) }
func (TestConnection) getConnections() ([]string, error)     { panic(errorNotSupported) }
func (TestConnection) hijackConnection(string) Connection    { panic(errorNotSupported) }
func (TestConnection) closeStaleConnection() error           { panic(errorNotSupported) }
func (TestConnection) getConsumingQueues() ([]string, error) { panic(errorNotSupported) }
func (TestConnection) unlistAllQueues() error                { panic(errorNotSupported) }
func (TestConnection) openQueue(string) Queue                { panic(errorNotSupported) }
func (TestConnection) stopHeartbeat() error                  { panic(errorNotSupported) }
func (TestConnection) flushDb() error                        { panic(errorNotSupported) }

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
