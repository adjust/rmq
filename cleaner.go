package queue

import (
	"fmt"
	"log"
)

type Cleaner struct {
	connection *Connection
}

func NewCleaner(connection *Connection) *Cleaner {
	return &Cleaner{connection: connection}
}

func (cleaner *Cleaner) Clean() error {
	connectionNames := cleaner.connection.GetConnections()
	for _, connectionName := range connectionNames {
		connection := cleaner.connection.hijackConnection(connectionName)
		if connection.Check() {
			continue // skip active connections!
		}

		if err := cleaner.CleanConnection(connection); err != nil {
			return err
		}
	}

	return nil
}

func (cleaner *Cleaner) CleanConnection(connection *Connection) error {
	queueNames := connection.GetConsumingQueues()
	for _, queueName := range queueNames {
		queue := connection.OpenQueue(queueName)
		cleaner.CleanQueue(queue)
	}

	if !connection.Close() {
		return fmt.Errorf("queue cleaner failed to close connection %s", connection)
	}

	if err := connection.CloseAllQueuesInConnection(); err != nil {
		return fmt.Errorf("queue cleaner failed to close all queues %d %s", connection, err)
	}

	log.Printf("queue cleaner cleaned connection %s", connection)
	return nil
}

func (cleaner *Cleaner) CleanQueue(queue *Queue) {
	returned := queue.ReturnAllUnackedDeliveries()
	queue.CloseInConnection()
	log.Printf("queue cleaner cleaned queue %s %d", queue, returned)
}
