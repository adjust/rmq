package queue

type TestQueue struct {
	LastDeliveries []string
}

func NewTestQueue() *TestQueue {
	queue := &TestQueue{}
	queue.Reset()
	return queue
}

func (queue *TestQueue) Publish(payload string) bool {
	queue.LastDeliveries = append(queue.LastDeliveries, payload)
	return true
}

func (queue *TestQueue) Reset() {
	queue.LastDeliveries = []string{}
}
