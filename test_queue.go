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

func (queue *TestQueue) StartConsuming(prefetchLimit int) bool {
	return true
}

func (queue *TestQueue) AddConsumer(tag string, consumer Consumer) string {
	return ""
}

func (queue *TestQueue) ReturnAllRejectedDeliveries() int {
	return 0
}

func (queue *TestQueue) PurgeReady() int {
	return 0
}

func (queue *TestQueue) Reset() {
	queue.LastDeliveries = []string{}
}
