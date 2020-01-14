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

func (queue *TestQueue) Publish(payload ...string) bool {
	queue.LastDeliveries = append(queue.LastDeliveries, payload...)
	return true
}

func (queue *TestQueue) PublishBytes(payload ...[]byte) bool {
	stringifiedBytes := make([]string, len(payload))
	for i, b := range payload {
		stringifiedBytes[i] = string(b)
	}
	return queue.Publish(stringifiedBytes...)
}

func (queue *TestQueue) SetPushQueue(pushQueue Queue) {
}

func (queue *TestQueue) StartConsuming(prefetchLimit int, pollDuration time.Duration) bool {
	return true
}

func (queue *TestQueue) StopConsuming() <-chan struct{} {
	return nil
}

func (queue *TestQueue) AddConsumer(tag string, consumer Consumer) string {
	return ""
}

func (queue *TestQueue) AddConsumerFunc(tag string, consumerFunc ConsumerFunc) string {
	return ""
}

func (queue *TestQueue) AddBatchConsumer(tag string, batchSize int, consumer BatchConsumer) string {
	return ""
}

func (queue *TestQueue) AddBatchConsumerWithTimeout(tag string, batchSize int, timeout time.Duration, consumer BatchConsumer) string {
	return ""
}

func (queue *TestQueue) ReturnRejected(count int) int {
	return 0
}

func (queue *TestQueue) ReturnAllRejected() int {
	return 0
}

func (queue *TestQueue) PurgeReady() int {
	return 0
}

func (queue *TestQueue) PurgeRejected() int {
	return 0
}

func (queue *TestQueue) Close() bool {
	return false
}

func (queue *TestQueue) Reset() {
	queue.LastDeliveries = []string{}
}
