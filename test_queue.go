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

func (queue *TestQueue) Publish(payload ...string) error {
	queue.LastDeliveries = append(queue.LastDeliveries, payload...)
	return nil
}

func (queue *TestQueue) PublishBytes(payload ...[]byte) error {
	stringifiedBytes := make([]string, len(payload))
	for i, b := range payload {
		stringifiedBytes[i] = string(b)
	}
	return queue.Publish(stringifiedBytes...)
}

func (*TestQueue) SetPushQueue(Queue)                                   { panic(errorNotSupported) }
func (*TestQueue) StartConsuming(int64, time.Duration) error            { panic(errorNotSupported) }
func (*TestQueue) StopConsuming() <-chan struct{}                       { panic(errorNotSupported) }
func (*TestQueue) AddConsumer(string, Consumer) (string, error)         { panic(errorNotSupported) }
func (*TestQueue) AddConsumerFunc(string, ConsumerFunc) (string, error) { panic(errorNotSupported) }
func (*TestQueue) AddBatchConsumer(string, int64, time.Duration, BatchConsumer) (string, error) {
	panic(errorNotSupported)
}
func (*TestQueue) AddBatchConsumerFunc(string, int64, time.Duration, BatchConsumerFunc) (string, error) {
	panic(errorNotSupported)
}
func (*TestQueue) ReturnUnacked(int64) (int64, error)  { panic(errorNotSupported) }
func (*TestQueue) ReturnRejected(int64) (int64, error) { panic(errorNotSupported) }
func (*TestQueue) PurgeReady() (int64, error)          { panic(errorNotSupported) }
func (*TestQueue) PurgeRejected() (int64, error)       { panic(errorNotSupported) }
func (*TestQueue) Destroy() (int64, int64, error)      { panic(errorNotSupported) }
func (*TestQueue) Drain(count int64) ([]string, error) { panic(errorNotSupported) }
func (*TestQueue) closeInStaleConnection() error       { panic(errorNotSupported) }
func (*TestQueue) readyCount() (int64, error)          { panic(errorNotSupported) }
func (*TestQueue) unackedCount() (int64, error)        { panic(errorNotSupported) }
func (*TestQueue) rejectedCount() (int64, error)       { panic(errorNotSupported) }
func (*TestQueue) getConsumers() ([]string, error)     { panic(errorNotSupported) }

// test helper

func (queue *TestQueue) Reset() {
	queue.LastDeliveries = []string{}
}
