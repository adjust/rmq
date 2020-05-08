package rmq

import "math"

type Cleaner struct {
	connection Connection
}

func NewCleaner(connection Connection) *Cleaner {
	return &Cleaner{connection: connection}
}

func (cleaner *Cleaner) Clean() (returned int64, err error) {
	connectionNames, err := cleaner.connection.getConnections()
	if err != nil {
		return 0, err
	}

	for _, connectionName := range connectionNames {
		hijackedConnection := cleaner.connection.hijackConnection(connectionName)
		switch err := hijackedConnection.checkHeartbeat(); err {
		case nil: // active connection
			continue
		case ErrorNotFound:
			n, err := cleanConnection(hijackedConnection)
			if err != nil {
				return 0, err
			}
			returned += n
		default:
			return 0, err
		}
	}

	return returned, nil
}

func cleanConnection(hijackedConnection Connection) (returned int64, err error) {
	queueNames, err := hijackedConnection.getConsumingQueues()
	if err != nil {
		return 0, err
	}

	for _, queueName := range queueNames {
		queue, err := hijackedConnection.OpenQueue(queueName)
		if err != nil {
			return 0, err
		}

		n, err := cleanQueue(queue)
		if err != nil {
			return 0, err
		}

		returned += n
	}

	if err := hijackedConnection.closeStaleConnection(); err != nil {
		return 0, err
	}

	// log.Printf("rmq cleaner cleaned connection %s", hijackedConnection)
	return returned, nil
}

func cleanQueue(queue Queue) (returned int64, err error) {
	returned, err = queue.ReturnUnacked(math.MaxInt64)
	if err != nil {
		return 0, err
	}
	if err := queue.closeInStaleConnection(); err != nil {
		return 0, err
	}
	// log.Printf("rmq cleaner cleaned queue %s %d", queue, returned)
	return returned, nil
}
