package rmq

type Cleaner struct {
	connection Connection
}

func NewCleaner(connection Connection) *Cleaner {
	return &Cleaner{connection: connection}
}

func (cleaner *Cleaner) Clean() error {
	connectionNames, err := cleaner.connection.getConnections()
	if err != nil {
		return err
	}

	for _, connectionName := range connectionNames {
		connection := cleaner.connection.hijackConnection(connectionName)
		switch err := connection.check(); err {
		case nil: // active connection
			continue
		case ErrorNotFound:
			if err := CleanConnection(connection); err != nil {
				return err
			}
		default:
			return err
		}
	}

	return nil
}

func CleanConnection(connection Connection) error {
	queueNames, err := connection.getConsumingQueues()
	if err != nil {
		return err
	}

	for _, queueName := range queueNames {
		queue, err := connection.OpenQueue(queueName)
		if err != nil {
			return err
		}

		if err := CleanQueue(queue); err != nil {
			return err
		}
	}

	if err := connection.closeStaleConnection(); err != nil {
		return err
	}

	// log.Printf("rmq cleaner cleaned connection %s", connection)
	return nil
}

func CleanQueue(queue Queue) error {
	returned, err := queue.ReturnAllUnacked()
	if err != nil {
		return err
	}
	queue.closeInConnection()
	_ = returned
	// log.Printf("rmq cleaner cleaned queue %s %d", queue, returned)
	return nil
}
