package rmq

import "fmt"

type Cleaner struct {
	connection Connection
}

func NewCleaner(connection Connection) *Cleaner {
	return &Cleaner{connection: connection}
}

func (cleaner *Cleaner) Clean() error {
	cleanerConnection, ok := cleaner.connection.(*redisConnection)
	if !ok {
		return nil
	}
	connectionNames := cleanerConnection.GetConnections()
	for _, connectionName := range connectionNames {
		connection := cleanerConnection.hijackConnection(connectionName)
		if connection.Check() {
			continue // skip active connections!
		}

		if err := CleanConnection(connection); err != nil {
			return err
		}
	}

	return nil
}

func CleanConnection(connection *redisConnection) error {
	queueNames := connection.GetConsumingQueues()
	for _, queueName := range queueNames {
		queue, ok := connection.OpenQueue(queueName).(*redisQueue)
		if !ok {
			return fmt.Errorf("rmq cleaner failed to open queue %s", queueName)
		}

		CleanQueue(queue)
	}

	if !connection.Close() {
		return fmt.Errorf("rmq cleaner failed to close connection %s", connection)
	}

	if err := connection.CloseAllQueuesInConnection(); err != nil {
		return fmt.Errorf("rmq cleaner failed to close all queues %s %s", connection, err)
	}

	// log.Printf("rmq cleaner cleaned connection %s", connection)
	return nil
}

func CleanQueue(queue *redisQueue) {
	returned := queue.ReturnAllUnacked()
	queue.CloseInConnection()
	_ = returned
	// log.Printf("rmq cleaner cleaned queue %s %d", queue, returned)
}
