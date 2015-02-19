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
		if err := cleaner.CleanQueue(queue); err != nil {
			return err
		}
	}

	if err := connection.Close(); err != nil {
		return fmt.Errorf("queue cleaner failed to close connection %s %s", connection, err)
	}

	closedQueues := connection.CloseAllQueuesInConnection()
	if closedQueues != len(queueNames) {
		return fmt.Errorf("queue cleaner closed unexpected number of queues %d %s", closedQueues, queueNames)
	}

	log.Printf("queue cleaner cleaned connection %s", connection)
	return nil
}

func (cleaner *Cleaner) CleanQueue(queue *Queue) error {
	returned, err := queue.ReturnUnackedDeliveries()
	if err != nil {
		return fmt.Errorf("queue cleaner failed to return unacked deliveries of queue %s %s", queue, err)
	}

	if err := queue.CloseInConnection(); err != nil {
		return fmt.Errorf("queue cleaner failed to close queue in connection %s %s", queue, err)
	}

	log.Printf("queue cleaner cleaned queue %s %d", queue, returned)
	return nil
}
