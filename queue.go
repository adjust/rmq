package queue

type Queue struct {
	name       string
	connection *Connection
}

func newQueue(name string, connection *Connection) *Queue {
	queue := &Queue{
		name:       name,
		connection: connection,
	}
	return queue
}
