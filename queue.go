package rmq

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

const (
	defaultBatchTimeout = time.Second
	purgeBatchSize      = int64(100)
)

type Queue interface {
	Publish(payload ...string) error
	PublishBytes(payload ...[]byte) error
	SetPushQueue(pushQueue Queue)
	StartConsuming(prefetchLimit int64, pollDuration time.Duration) error
	StopConsuming() <-chan struct{}
	AddConsumer(tag string, consumer Consumer) (string, error)
	AddConsumerFunc(tag string, consumerFunc ConsumerFunc) (string, error)
	AddBatchConsumer(tag string, batchSize int64, timeout time.Duration, consumer BatchConsumer) (string, error)
	AddBatchConsumerFunc(tag string, batchSize int64, timeout time.Duration, batchConsumerFunc BatchConsumerFunc) (string, error)
	PurgeReady() (int64, error)
	PurgeRejected() (int64, error)
	ReturnUnacked(max int64) (int64, error)
	ReturnRejected(max int64) (int64, error)
	Destroy() (readyCount, rejectedCount int64, err error)
	Drain(count int64) ([]string, error)

	// internals
	// used in cleaner
	closeInStaleConnection() error
	// used for stats
	readyCount() (int64, error)
	unackedCount() (int64, error)
	rejectedCount() (int64, error)
	getConsumers() ([]string, error)
}

type redisQueue struct {
	name           string
	connectionName string
	queuesKey      string // key to list of queues consumed by this connection
	consumersKey   string // key to set of consumers using this connection
	unackedKey     string // key to list of currently consuming deliveries
	readyKey       string // key to list of ready deliveries
	rejectedKey    string // key to list of rejected deliveries
	pushKey        string // key to list of pushed deliveries
	redisClient    RedisClient
	errChan        chan<- error
	prefetchLimit  int64 // max number of prefetched deliveries number of unacked can go up to prefetchLimit + numConsumers
	pollDuration   time.Duration

	lock             sync.Mutex    // protects the fields below related to starting and stopping this queue
	consumingStopped chan struct{} // this chan gets closed when consuming on this queue has stopped
	stopWg           sync.WaitGroup
	ackCtx           context.Context
	ackCancel        context.CancelFunc
	deliveryChan     chan Delivery // nil for publish channels, not nil for consuming channels
}

func newQueue(
	name, connectionName, queuesKey string,
	consumersTemplate, unackedTemplate, readyTemplate, rejectedTemplate string,
	redisClient RedisClient,
	errChan chan<- error,
) *redisQueue {

	consumersKey := strings.Replace(consumersTemplate, phConnection, connectionName, 1)
	consumersKey = strings.Replace(consumersKey, phQueue, name, 1)

	unackedKey := strings.Replace(unackedTemplate, phConnection, connectionName, 1)
	unackedKey = strings.Replace(unackedKey, phQueue, name, 1)

	readyKey := strings.Replace(readyTemplate, phQueue, name, 1)
	rejectedKey := strings.Replace(rejectedTemplate, phQueue, name, 1)

	consumingStopped := make(chan struct{})
	ackCtx, ackCancel := context.WithCancel(context.Background())

	queue := &redisQueue{
		name:             name,
		connectionName:   connectionName,
		queuesKey:        queuesKey,
		consumersKey:     consumersKey,
		unackedKey:       unackedKey,
		readyKey:         readyKey,
		rejectedKey:      rejectedKey,
		redisClient:      redisClient,
		errChan:          errChan,
		consumingStopped: consumingStopped,
		ackCtx:           ackCtx,
		ackCancel:        ackCancel,
	}
	return queue
}

func (queue *redisQueue) String() string {
	return fmt.Sprintf("[%s conn:%s]", queue.name, queue.connectionName)
}

// Publish adds a delivery with the given payload to the queue
// returns how many deliveries are in the queue afterwards
func (queue *redisQueue) Publish(payload ...string) error {
	_, err := queue.redisClient.LPush(queue.readyKey, payload...)
	return err
}

// PublishBytes just casts the bytes and calls Publish
func (queue *redisQueue) PublishBytes(payload ...[]byte) error {
	stringifiedBytes := make([]string, len(payload))
	for i, b := range payload {
		stringifiedBytes[i] = string(b)
	}
	return queue.Publish(stringifiedBytes...)
}

// SetPushQueue sets a push queue. In the consumer function you can call
// delivery.Push(). If a push queue is set the delivery then gets moved from
// the original queue to the push queue. If no push queue is set it's
// equivalent to calling delivery.Reject().
// NOTE: panics if pushQueue is not a *redisQueue
func (queue *redisQueue) SetPushQueue(pushQueue Queue) {
	queue.pushKey = pushQueue.(*redisQueue).readyKey
}

// StartConsuming starts consuming into a channel of size prefetchLimit
// must be called before consumers can be added!
// pollDuration is the duration the queue sleeps before checking for new deliveries
func (queue *redisQueue) StartConsuming(prefetchLimit int64, pollDuration time.Duration) error {
	queue.lock.Lock()
	defer queue.lock.Unlock()

	// If deliveryChan is set, then we are already consuming
	if queue.deliveryChan != nil {
		return ErrorAlreadyConsuming
	}
	select {
	case <-queue.consumingStopped:
		// If consuming is stopped then we must not try to
		return ErrorConsumingStopped
	default:
	}

	// add queue to list of queues consumed on this connection
	if _, err := queue.redisClient.SAdd(queue.queuesKey, queue.name); err != nil {
		return err
	}

	queue.prefetchLimit = prefetchLimit
	queue.pollDuration = pollDuration
	queue.deliveryChan = make(chan Delivery, prefetchLimit)
	// log.Printf("rmq queue started consuming %s %d %s", queue, prefetchLimit, pollDuration)
	go queue.consume()
	return nil
}

func (queue *redisQueue) consume() {
	errorCount := 0 // number of consecutive batch errors

	for {
		switch err := queue.consumeBatch(); err {
		case nil: // success
			errorCount = 0

		case ErrorConsumingStopped:
			close(queue.deliveryChan)
			return

		default: // redis error
			errorCount++
			select { // try to add error to channel, but don't block
			case queue.errChan <- &ConsumeError{RedisErr: err, Count: errorCount}:
			default:
			}
		}
		time.Sleep(jitteredDuration(queue.pollDuration))
	}
}

func (queue *redisQueue) consumeBatch() error {
	select {
	case <-queue.consumingStopped:
		return ErrorConsumingStopped
	default:
	}

	// unackedCount == <deliveries in deliveryChan> + <deliveries in Consume()>
	unackedCount, err := queue.unackedCount()
	if err != nil {
		return err
	}

	batchSize := queue.prefetchLimit - unackedCount
	if batchSize <= 0 {
		return nil
	}

	for i := int64(0); i < batchSize; i++ {
		select {
		case <-queue.consumingStopped:
			return ErrorConsumingStopped
		default:
		}

		payload, err := queue.redisClient.RPopLPush(queue.readyKey, queue.unackedKey)
		if err == ErrorNotFound {
			return nil
		}
		if err != nil {
			return err
		}

		d, err := queue.newDelivery(payload)
		if err != nil {
			return fmt.Errorf("create new delivery: %w", err)
		}

		queue.deliveryChan <- d
	}

	return nil
}

func (queue *redisQueue) newDelivery(payload string) (Delivery, error) {
	rd := &redisDelivery{
		ctx:         queue.ackCtx,
		payload:     payload,
		unackedKey:  queue.unackedKey,
		rejectedKey: queue.rejectedKey,
		pushKey:     queue.pushKey,
		redisClient: queue.redisClient,
		errChan:     queue.errChan,
	}

	var err error
	rd.header, rd.clearPayload, err = ExtractHeaderAndPayload(payload)
	if err == nil {
		return rd, nil
	}

	// we need to reject a delivery here to move the delivery from the unacked to the rejected list.
	rejectErr := rd.Reject()
	if rejectErr != nil {
		return nil, fmt.Errorf("%s, reject faulty delivery: %w", err, rejectErr)
	}

	return nil, err
}

// StopConsuming can be used to stop all consumers on this queue. It returns a
// channel which can be used to wait for all active consumers to finish their
// current Consume() call. This is useful to implement graceful shutdown.
func (queue *redisQueue) StopConsuming() <-chan struct{} {
	finishedChan := make(chan struct{})
	// We only stop consuming once
	// This function returns immediately, while the work of actually stopping runs in a separate goroutine
	go func() {
		queue.lock.Lock()
		defer queue.lock.Unlock()

		select {
		case <-queue.consumingStopped:
			// already stopped, nothing to do
			close(finishedChan)
			return
		default:
			close(queue.consumingStopped)
			queue.ackCancel()
			queue.stopWg.Wait()
			close(finishedChan)
		}
	}()

	return finishedChan
}

// AddConsumer adds a consumer to the queue and returns its internal name
func (queue *redisQueue) AddConsumer(tag string, consumer Consumer) (name string, err error) {
	name, err = queue.addConsumer(tag)
	if err != nil {
		return "", err
	}
	go queue.consumerConsume(consumer)
	return name, nil
}

func (queue *redisQueue) consumerConsume(consumer Consumer) {
	defer func() {
		queue.stopWg.Done()
	}()
	for {
		select {
		case <-queue.consumingStopped: // prefer this case
			return
		default:
		}

		select {
		case <-queue.consumingStopped:
			return

		case delivery, ok := <-queue.deliveryChan:
			if !ok { // deliveryChan closed
				return
			}

			consumer.Consume(delivery)
		}
	}
}

// AddConsumerFunc adds a consumer which is defined only by a function. This is
// similar to http.HandlerFunc and useful if your consumers don't need any
// state.
func (queue *redisQueue) AddConsumerFunc(tag string, consumerFunc ConsumerFunc) (string, error) {
	return queue.AddConsumer(tag, consumerFunc)
}

// AddBatchConsumer is similar to AddConsumer, but for batches of deliveries
// timeout limits the amount of time waiting to fill an entire batch
// The timer is only started when the first message in a batch is received
func (queue *redisQueue) AddBatchConsumer(tag string, batchSize int64, timeout time.Duration, consumer BatchConsumer) (string, error) {
	name, err := queue.addConsumer(tag)
	if err != nil {
		return "", err
	}
	go queue.consumerBatchConsume(batchSize, timeout, consumer)
	return name, nil
}

// AddBatchConsumerFunc is similar to AddConsumerFunc, but for batches of deliveries
// timeout limits the amount of time waiting to fill an entire batch
// The timer is only started when the first message in a batch is received
func (queue *redisQueue) AddBatchConsumerFunc(tag string, batchSize int64, timeout time.Duration, batchConsumerFunc BatchConsumerFunc) (string, error) {
	name, err := queue.addConsumer(tag)
	if err != nil {
		return "", err
	}
	go queue.consumerBatchConsume(batchSize, timeout, batchConsumerFunc)
	return name, nil
}

func (queue *redisQueue) consumerBatchConsume(batchSize int64, timeout time.Duration, consumer BatchConsumer) {
	defer func() {
		queue.stopWg.Done()
	}()
	batch := []Delivery{}
	for {
		select {
		case <-queue.consumingStopped: // prefer this case
			return
		default:
		}

		select {
		case <-queue.consumingStopped:
			return

		case delivery, ok := <-queue.deliveryChan: // Wait for first delivery
			if !ok { // deliveryChan closed
				return
			}

			batch = append(batch, delivery)
			batch, ok = queue.batchTimeout(batchSize, batch, timeout)
			if !ok {
				return
			}

			consumer.Consume(batch)
			batch = batch[:0] // reset batch
		}
	}
}

func (queue *redisQueue) batchTimeout(batchSize int64, batch []Delivery, timeout time.Duration) (fullBatch []Delivery, ok bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-queue.consumingStopped: // prefer this case
			return nil, false
		default:
		}

		select {
		case <-queue.consumingStopped: // consuming stopped: abort batch
			return nil, false

		case <-timer.C: // timeout: submit batch
			return batch, true

		case delivery, ok := <-queue.deliveryChan:
			if !ok { // deliveryChan closed: abort batch
				return nil, false
			}

			batch = append(batch, delivery)
			if int64(len(batch)) >= batchSize {
				return batch, true // once big enough: submit batch
			}
		}
	}
}

func (queue *redisQueue) addConsumer(tag string) (name string, err error) {
	queue.lock.Lock()
	defer queue.lock.Unlock()

	if err := queue.ensureConsuming(); err != nil {
		return "", err
	}

	name = fmt.Sprintf("%s-%s", tag, RandomString(6))

	// add consumer to list of consumers of this queue
	if _, err := queue.redisClient.SAdd(queue.consumersKey, name); err != nil {
		return "", err
	}

	queue.stopWg.Add(1)
	// log.Printf("rmq queue added consumer %s %s", queue, name)
	return name, nil
}

// PurgeReady removes all ready deliveries from the queue and returns the number of purged deliveries
func (queue *redisQueue) PurgeReady() (int64, error) {
	return queue.deleteRedisList(queue.readyKey)
}

// PurgeRejected removes all rejected deliveries from the queue and returns the number of purged deliveries
func (queue *redisQueue) PurgeRejected() (int64, error) {
	return queue.deleteRedisList(queue.rejectedKey)
}

// return number of deleted list items
// https://www.redisgreen.net/blog/deleting-large-lists
func (queue *redisQueue) deleteRedisList(key string) (int64, error) {
	total, err := queue.redisClient.LLen(key)
	if total == 0 {
		return 0, err // nothing to do
	}

	// delete elements without blocking
	for todo := total; todo > 0; todo -= purgeBatchSize {
		// minimum of purgeBatchSize and todo
		batchSize := purgeBatchSize
		if batchSize > todo {
			batchSize = todo
		}

		// remove one batch
		err := queue.redisClient.LTrim(key, 0, -1-batchSize)
		if err != nil {
			return 0, err
		}
	}

	return total, nil
}

// ReturnUnacked tries to return max unacked deliveries back to
// the ready queue and returns the number of returned deliveries
func (queue *redisQueue) ReturnUnacked(max int64) (count int64, error error) {
	return queue.move(queue.unackedKey, queue.readyKey, max)
}

// ReturnRejected tries to return max rejected deliveries back to
// the ready queue and returns the number of returned deliveries
func (queue *redisQueue) ReturnRejected(max int64) (count int64, err error) {
	return queue.move(queue.rejectedKey, queue.readyKey, max)
}

func (queue *redisQueue) move(from, to string, max int64) (n int64, error error) {
	for n = 0; n < max; n++ {
		switch _, err := queue.redisClient.RPopLPush(from, to); err {
		case nil: // moved one
			continue
		case ErrorNotFound: // nothing left
			return n, nil
		default: // error
			return 0, err
		}
	}
	return n, nil
}

// Drain removes and returns 'count' elements from the queue. In case of an error,
// Drain return all elements removed until the error occurred and the error itself.
func (queue *redisQueue) Drain(count int64) ([]string, error) {
	var (
		n   int64
		err error
	)
	out := make([]string, 0, count)

	for n = 0; n < count; n++ {
		val, err := queue.redisClient.RPop(queue.readyKey)
		if err != nil {
			return out, err
		}
		out = append(out, val)
	}

	return out, err
}

// Destroy purges and removes the queue from the list of queues
func (queue *redisQueue) Destroy() (readyCount, rejectedCount int64, err error) {
	readyCount, err = queue.PurgeReady()
	if err != nil {
		return 0, 0, err
	}
	rejectedCount, err = queue.PurgeRejected()
	if err != nil {
		return 0, 0, err
	}

	count, err := queue.redisClient.SRem(queuesKey, queue.name)
	if err != nil {
		return 0, 0, err
	}
	if count == 0 {
		return 0, 0, ErrorNotFound
	}

	return readyCount, rejectedCount, nil
}

// closeInStaleConnection closes the queue in the associated connection by removing all related keys
// not supposed to be called on queues in active sessions
func (queue *redisQueue) closeInStaleConnection() error {
	if _, err := queue.redisClient.Del(queue.unackedKey); err != nil {
		return err
	}
	if _, err := queue.redisClient.Del(queue.consumersKey); err != nil {
		return err
	}

	count, err := queue.redisClient.SRem(queue.queuesKey, queue.name)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrorNotFound
	}

	return nil
}

func (queue *redisQueue) readyCount() (int64, error) {
	return queue.redisClient.LLen(queue.readyKey)
}

func (queue *redisQueue) unackedCount() (int64, error) {
	return queue.redisClient.LLen(queue.unackedKey)
}

func (queue *redisQueue) rejectedCount() (int64, error) {
	return queue.redisClient.LLen(queue.rejectedKey)
}

func (queue *redisQueue) getConsumers() ([]string, error) {
	return queue.redisClient.SMembers(queue.consumersKey)
}

// The caller of this method should be holding the queue.lock mutex
func (queue *redisQueue) ensureConsuming() error {
	if queue.deliveryChan == nil {
		return ErrorNotConsuming
	}
	select {
	case <-queue.consumingStopped:
		return ErrorConsumingStopped
	default:
		return nil
	}
}

// jitteredDuration calculates and returns a value that is +/-10% the input duration
func jitteredDuration(duration time.Duration) time.Duration {
	factor := 0.9 + rand.Float64()*0.2 // a jitter factor between 0.9 and 1.1 (+-10%)
	return time.Duration(float64(duration) * factor)
}
