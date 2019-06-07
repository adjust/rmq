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

func (queue *TestQueue) Publish(payload ...string) (bool, error) {
	queue.LastDeliveries = append(queue.LastDeliveries, payload...)
	return true, nil
}

func (queue *TestQueue) PublishBytes(payload ...[]byte) (bool, error) {
	stringifiedBytes := make([]string, len(payload))
	for i, b := range payload {
		stringifiedBytes[i] = string(b)
	}
	return queue.Publish(stringifiedBytes...)
}

func (queue *TestQueue) SetPushQueue(pushQueue Queue) {
}

func (queue *TestQueue) StartConsuming(prefetchLimit int, pollDuration time.Duration) error {
	return nil
}

func (queue *TestQueue) StopConsuming() <-chan struct{} {
	return nil
}

func (queue *TestQueue) AddConsumer(tag string, consumer Consumer) (string, error) {
	return "", nil
}

func (queue *TestQueue) AddConsumerFunc(tag string, consumerFunc ConsumerFunc) (string, error) {
	return "", nil
}

func (queue *TestQueue) AddBatchConsumer(tag string, batchSize int, consumer BatchConsumer) (string, error) {
	return "", nil
}

func (queue *TestQueue) AddBatchConsumerWithTimeout(tag string, batchSize int, timeout time.Duration, consumer BatchConsumer) (string, error) {
	return "", nil
}

func (queue *TestQueue) ReturnRejected(count int) (int, error) {
	return 0, nil
}

func (queue *TestQueue) ReturnAllRejected() (int, error) {
	return 0, nil
}

func (queue *TestQueue) PurgeReady() (int, error) {
	return 0, nil
}

func (queue *TestQueue) PurgeRejected() (int, error) {
	return 0, nil
}

func (queue *TestQueue) Close() (bool, error) {
	return false, nil
}

func (queue *TestQueue) Reset() {
	queue.LastDeliveries = []string{}
}
