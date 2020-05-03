package rmq

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adjust/uniuri"
)

var (
	ErrorAlreadyConsuming = errors.New("must not call StartConsuming() multiple times")
	ErrorNotConsuming     = errors.New("must call StartConsuming() before adding consumers")
)

const (
	connectionsKey                   = "rmq::connections"                                           // Set of connection names
	connectionHeartbeatTemplate      = "rmq::connection::{connection}::heartbeat"                   // expires after {connection} died
	connectionQueuesTemplate         = "rmq::connection::{connection}::queues"                      // Set of queues consumers of {connection} are consuming
	connectionQueueConsumersTemplate = "rmq::connection::{connection}::queue::[{queue}]::consumers" // Set of all consumers from {connection} consuming from {queue}
	connectionQueueUnackedTemplate   = "rmq::connection::{connection}::queue::[{queue}]::unacked"   // List of deliveries consumers of {connection} are currently consuming

	queuesKey             = "rmq::queues"                     // Set of all open queues
	queueReadyTemplate    = "rmq::queue::[{queue}]::ready"    // List of deliveries in that {queue} (right is first and oldest, left is last and youngest)
	queueRejectedTemplate = "rmq::queue::[{queue}]::rejected" // List of rejected deliveries from that {queue}

	phConnection = "{connection}" // connection name
	phQueue      = "{queue}"      // queue name
	phConsumer   = "{consumer}"   // consumer name (consisting of tag and token)

	defaultBatchTimeout = time.Second
	purgeBatchSize      = int64(100)
)

type Queue interface {
	Publish(payload ...string) (total int64, err error)
	PublishBytes(payload ...[]byte) (total int64, err error)
	SetPushQueue(pushQueue Queue)
	StartConsuming(prefetchLimit int64, pollDuration time.Duration) error
	StopConsuming() <-chan struct{}
	AddConsumer(tag string, consumer Consumer) (string, error)
	AddConsumerFunc(tag string, consumerFunc ConsumerFunc) (string, error)
	AddBatchConsumer(tag string, batchSize int64, consumer BatchConsumer) (string, error)
	AddBatchConsumerWithTimeout(tag string, batchSize int64, timeout time.Duration, consumer BatchConsumer) (string, error)
	PurgeReady() (int64, error)
	PurgeRejected() (int64, error)
	ReturnRejected(count int64) (int64, error)
	ReturnAllUnacked() (int64, error)
	ReturnAllRejected() (int64, error)
	Destroy() (readyCount, rejectedCount int64, err error)

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
	name             string
	connectionName   string
	queuesKey        string // key to list of queues consumed by this connection
	consumersKey     string // key to set of consumers using this connection
	readyKey         string // key to list of ready deliveries
	rejectedKey      string // key to list of rejected deliveries
	unackedKey       string // key to list of currently consuming deliveries
	pushKey          string // key to list of pushed deliveries
	redisClient      RedisClient
	deliveryChan     chan Delivery // nil for publish channels, not nil for consuming channels
	prefetchLimit    int64         // max number of prefetched deliveries number of unacked can go up to prefetchLimit + numConsumers
	pollDuration     time.Duration
	consumingStopped int32 // queue status, 1 for stopped, 0 for consuming
	stopWg           sync.WaitGroup
}

func newQueue(name, connectionName, queuesKey string, redisClient RedisClient) *redisQueue {
	consumersKey := strings.Replace(connectionQueueConsumersTemplate, phConnection, connectionName, 1)
	consumersKey = strings.Replace(consumersKey, phQueue, name, 1)

	readyKey := strings.Replace(queueReadyTemplate, phQueue, name, 1)
	rejectedKey := strings.Replace(queueRejectedTemplate, phQueue, name, 1)

	unackedKey := strings.Replace(connectionQueueUnackedTemplate, phConnection, connectionName, 1)
	unackedKey = strings.Replace(unackedKey, phQueue, name, 1)

	queue := &redisQueue{
		name:             name,
		connectionName:   connectionName,
		queuesKey:        queuesKey,
		consumersKey:     consumersKey,
		readyKey:         readyKey,
		rejectedKey:      rejectedKey,
		unackedKey:       unackedKey,
		redisClient:      redisClient,
		consumingStopped: 1, // start with stopped status
	}
	return queue
}

func (queue *redisQueue) String() string {
	return fmt.Sprintf("[%s conn:%s]", queue.name, queue.connectionName)
}

// Publish adds a delivery with the given payload to the queue
// returns how many deliveries are in the queue afterwards
func (queue *redisQueue) Publish(payload ...string) (total int64, err error) {
	return queue.redisClient.LPush(queue.readyKey, payload...)
}

// PublishBytes just casts the bytes and calls Publish
func (queue *redisQueue) PublishBytes(payload ...[]byte) (total int64, err error) {
	stringifiedBytes := make([]string, len(payload))
	for i, b := range payload {
		stringifiedBytes[i] = string(b)
	}
	return queue.Publish(stringifiedBytes...)
}

// PurgeReady removes all ready deliveries from the queue and returns the number of purged deliveries
func (queue *redisQueue) PurgeReady() (int64, error) {
	return queue.deleteRedisList(queue.readyKey)
}

// PurgeRejected removes all rejected deliveries from the queue and returns the number of purged deliveries
func (queue *redisQueue) PurgeRejected() (int64, error) {
	return queue.deleteRedisList(queue.rejectedKey)
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

func (queue *redisQueue) readyCount() (int64, error) {
	return queue.redisClient.LLen(queue.readyKey)
}

func (queue *redisQueue) unackedCount() (int64, error) {
	return queue.redisClient.LLen(queue.unackedKey)
}

func (queue *redisQueue) rejectedCount() (int64, error) {
	return queue.redisClient.LLen(queue.rejectedKey)
}

// ReturnAllUnacked moves all unacked deliveries back to the ready
// queue and deletes the unacked key afterwards, returns number of returned
// deliveries
func (queue *redisQueue) ReturnAllUnacked() (int64, error) {
	// TODO: consider not LLen here, just poppush until empty
	unackedCount, err := queue.redisClient.LLen(queue.unackedKey)
	if err != nil {
		return 0, err
	}

	for i := int64(0); i < unackedCount; i++ {
		// one consideration: in theory it might not finish if packages get constantly added
		// but that can probably be safely ignored
		switch _, err := queue.redisClient.RPopLPush(queue.unackedKey, queue.readyKey); err {
		case nil:
			// debug(fmt.Sprintf("rmq queue returned unacked delivery %s %s", count, queue.readyKey)) // COMMENTOUT
			continue
		case ErrorNotFound:
			return i, nil
		default: // err
			return 0, err
		}
	}

	return unackedCount, nil
}

// ReturnAllRejected moves all rejected deliveries back to the ready
// list and returns the number of returned deliveries
func (queue *redisQueue) ReturnAllRejected() (int64, error) {
	// TODO: just use maxint instead of getting the actual count?
	rejectedCount, err := queue.rejectedCount()
	if err != nil {
		return 0, err
	}
	return queue.ReturnRejected(rejectedCount)
}

// ReturnRejected tries to return count rejected deliveries back to
// the ready list and returns the number of returned deliveries
func (queue *redisQueue) ReturnRejected(count int64) (int64, error) {
	if count == 0 {
		return 0, nil
	}

	for i := int64(0); i < count; i++ {
		switch _, err := queue.redisClient.RPopLPush(queue.rejectedKey, queue.readyKey); err {
		case nil:
			// debug(fmt.Sprintf("rmq queue returned rejected delivery %s %s", value, queue.readyKey)) // COMMENTOUT
			continue
		case ErrorNotFound:
			return i, nil
		default: // error
			return 0, err
		}
	}

	return count, nil
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
	if _, err := queue.redisClient.SRem(queue.queuesKey, queue.name); err != nil {
		return err
	}
	return nil
}

func (queue *redisQueue) SetPushQueue(pushQueue Queue) {
	// TODO: can we avoid the type check here?
	redisPushQueue, ok := pushQueue.(*redisQueue)
	if !ok {
		return // TODO: return error? or just panic?
	}

	queue.pushKey = redisPushQueue.readyKey
}

// StartConsuming starts consuming into a channel of size prefetchLimit
// must be called before consumers can be added!
// pollDuration is the duration the queue sleeps before checking for new deliveries
func (queue *redisQueue) StartConsuming(prefetchLimit int64, pollDuration time.Duration) error {
	if queue.deliveryChan != nil {
		return ErrorAlreadyConsuming
	}

	// add queue to list of queues consumed on this connection
	// TODO: return number of queues being consumed in this connection?
	if _, err := queue.redisClient.SAdd(queue.queuesKey, queue.name); err != nil {
		return err
	}

	queue.prefetchLimit = prefetchLimit
	queue.pollDuration = pollDuration
	queue.deliveryChan = make(chan Delivery, prefetchLimit)
	atomic.StoreInt32(&queue.consumingStopped, 0)
	// log.Printf("rmq queue started consuming %s %d %s", queue, prefetchLimit, pollDuration)
	go queue.consume()
	return nil
}

func (queue *redisQueue) StopConsuming() <-chan struct{} {
	finishedChan := make(chan struct{})
	if queue.deliveryChan == nil || atomic.LoadInt32(&queue.consumingStopped) == int32(1) {
		close(finishedChan) // not consuming or already stopped
		return finishedChan
	}

	// log.Printf("rmq queue stopping %s", queue)
	atomic.StoreInt32(&queue.consumingStopped, 1)
	go func() {
		queue.stopWg.Wait()
		close(finishedChan)
		// log.Printf("rmq queue stopped consuming %s", queue)
	}()

	return finishedChan
}

// AddConsumer adds a consumer to the queue and returns its internal name
func (queue *redisQueue) AddConsumer(tag string, consumer Consumer) (name string, err error) {
	queue.stopWg.Add(1)
	name, err = queue.addConsumer(tag)
	if err != nil {
		return "", err
	}
	go queue.consumerConsume(consumer)
	return name, nil
}

func (queue *redisQueue) AddConsumerFunc(tag string, consumerFunc ConsumerFunc) (string, error) {
	return queue.AddConsumer(tag, consumerFunc)
}

// AddBatchConsumer is similar to AddConsumer, but for batches of deliveries
func (queue *redisQueue) AddBatchConsumer(tag string, batchSize int64, consumer BatchConsumer) (string, error) {
	return queue.AddBatchConsumerWithTimeout(tag, batchSize, defaultBatchTimeout, consumer)
}

// Timeout limits the amount of time waiting to fill an entire batch
// The timer is only started when the first message in a batch is received
func (queue *redisQueue) AddBatchConsumerWithTimeout(tag string, batchSize int64, timeout time.Duration, consumer BatchConsumer) (string, error) {
	queue.stopWg.Add(1)
	name, err := queue.addConsumer(tag)
	if err != nil {
		return "", err
	}
	go queue.consumerBatchConsume(batchSize, timeout, consumer)
	return name, nil
}

func (queue *redisQueue) getConsumers() ([]string, error) {
	return queue.redisClient.SMembers(queue.consumersKey)
}

func (queue *redisQueue) addConsumer(tag string) (name string, err error) {
	if queue.deliveryChan == nil {
		return "", ErrorNotConsuming
	}

	name = fmt.Sprintf("%s-%s", tag, uniuri.NewLen(6))

	// add consumer to list of consumers of this queue
	// TODO: return number of consumers in this connection?
	if _, err := queue.redisClient.SAdd(queue.consumersKey, name); err != nil {
		return "", err
	}

	// log.Printf("rmq queue added consumer %s %s", queue, name)
	return name, nil
}

func (queue *redisQueue) consume() {
	for {
		if atomic.LoadInt32(&queue.consumingStopped) == int32(1) {
			// log.Printf("rmq queue stopped consuming %s", queue)
			close(queue.deliveryChan)
			// log.Printf("rmq queue stopped fetching %s", queue)
			return
		}

		batchSize, err := queue.batchSize()
		if err != nil {
			// TODO
		}

		switch err := queue.consumeBatch(batchSize); err {
		case nil:
			continue
		case ErrorNotFound: // less than batchSize were found
			time.Sleep(queue.pollDuration)
		default: // error
			// TODO
		}
	}
}

func (queue *redisQueue) batchSize() (int64, error) {
	unackedCount, err := queue.unackedCount()
	if err != nil {
		return 0, err
	}
	prefetchLimit := queue.prefetchLimit - unackedCount

	// TODO: ignore ready count here and just return prefetchLimit? yes, and inline this function
	readyCount, err := queue.readyCount()
	if err != nil {
		return 0, err
	}
	if readyCount < prefetchLimit {
		return readyCount, nil
	}
	return prefetchLimit, nil
}

// consumeBatch tries to read batchSize deliveries, returns true if any and all were consumed
func (queue *redisQueue) consumeBatch(batchSize int64) error {
	if batchSize == 0 {
		return ErrorNotFound
	}

	// TODO: do one blocking call (to wait for first), then no n-1 nonblocking
	// ones, just stop (finished batch) once redis.nil is returned (nothing
	// else available yet). !wantMore in that case
	// TODO: pipeline (this is a hot path, but can consider for other usages of RPopLPush too)
	for i := int64(0); i < batchSize; i++ {
		value, err := queue.redisClient.RPopLPush(queue.readyKey, queue.unackedKey)
		if err != nil {
			return err // NOTE: can be ErrorNotFound
		}

		// debug(fmt.Sprintf("consume %d/%d %s %s", i, batchSize, value, queue)) // COMMENTOUT
		queue.deliveryChan <- newDelivery(value, queue.unackedKey, queue.rejectedKey, queue.pushKey, queue.redisClient)
	}

	// debug(fmt.Sprintf("rmq queue consumed batch %s %d", queue, batchSize)) // COMMENTOUT
	return nil
}

func (queue *redisQueue) consumerConsume(consumer Consumer) {
	for delivery := range queue.deliveryChan {
		// debug(fmt.Sprintf("consumer consume %s %s", delivery, consumer)) // COMMENTOUT
		consumer.Consume(delivery)
	}
	queue.stopWg.Done()
}

func (queue *redisQueue) consumerBatchConsume(batchSize int64, timeout time.Duration, consumer BatchConsumer) {
	defer queue.stopWg.Done()
	batch := []Delivery{}
	for {
		// Wait for first delivery
		delivery, ok := <-queue.deliveryChan
		if !ok {
			// debug("batch channel closed") // COMMENTOUT
			return
		}
		batch = append(batch, delivery)
		// debug(fmt.Sprintf("batch consume added delivery %d", len(batch))) // COMMENTOUT
		batch, ok = queue.batchTimeout(batchSize, batch, timeout)
		consumer.Consume(batch)
		if !ok {
			// debug("batch channel closed") // COMMENTOUT
			return
		}
		batch = batch[:0] // reset batch
	}
}

func (queue *redisQueue) batchTimeout(batchSize int64, batch []Delivery, timeout time.Duration) (fullBatch []Delivery, ok bool) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			// debug("batch timer fired") // COMMENTOUT
			// debug(fmt.Sprintf("batch consume consume %d", len(batch))) // COMMENTOUT
			return batch, true
		case delivery, ok := <-queue.deliveryChan:
			if !ok {
				// debug("batch channel closed") // COMMENTOUT
				return batch, false
			}
			batch = append(batch, delivery)
			// debug(fmt.Sprintf("batch consume added delivery %d", len(batch))) // COMMENTOUT
			if int64(len(batch)) >= batchSize {
				// debug(fmt.Sprintf("batch consume wait %d < %d", len(batch), batchSize)) // COMMENTOUT
				return batch, true
			}
		}
	}
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

// TODO: remove this and all COMMENTOUT lines
func debug(message string) {
	// log.Printf("rmq debug: %s", message) // COMMENTOUT
}
