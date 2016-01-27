package rmq

import "time"

type TestQueue struct {
	name           string
	LastDeliveries []string
}

func NewTestQueue(name string) *TestQueue {
	queue := &TestQueue{name: name}
	queue.Reset()
	return queue
}

func (queue *TestQueue) String() string {
	return queue.name
}

func (queue *TestQueue) Publish(payload string) bool {
	queue.LastDeliveries = append(queue.LastDeliveries, payload)
	return true
}

func (queue *TestQueue) PublishBytes(payload []byte) bool {
	return queue.Publish(string(payload))
}

func (queue *TestQueue) SetPushQueue(pushQueue Queue) {
}

func (queue *TestQueue) StartConsuming(prefetchLimit int, pollDuration time.Duration) bool {
	return true
}

func (queue *TestQueue) StopConsuming() bool {
	return true
}

func (queue *TestQueue) AddConsumer(tag string, consumer Consumer) (name string, context *ConsumerContext) {
	return "", nil
}

func (queue *TestQueue) AddBatchConsumer(tag string, batchSize int, consumer BatchConsumer) string {
	return ""
}

func (queue *TestQueue) ReturnRejected(count int) int {
	return 0
}

func (queue *TestQueue) ReturnAllRejected() int {
	return 0
}

func (queue *TestQueue) PurgeReady() bool {
	return false
}

func (queue *TestQueue) PurgeRejected() bool {
	return false
}

func (queue *TestQueue) Close() bool {
	return false
}

func (queue *TestQueue) Reset() {
	queue.LastDeliveries = []string{}
}
