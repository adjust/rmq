package queue

import (
	"fmt"
	"log"
	"strings"
	"time"

	redis "github.com/adjust/redis-latest-head" // TODO: update
	"github.com/adjust/uniuri"
)

const (
	connectionsKey                   = "rmq::connections"                                         // Set of connection names
	connectionHeartbeatTemplate      = "rmq::connection::{connection}::heartbeat"                 // expires after {connection} died
	connectionQueuesTemplate         = "rmq::connection::{connection}::queues"                    // Set of queues consumers of {connection} are consuming
	connectionQueueConsumersTemplate = "rmq::connection::{connection}::queue::{queue}::consumers" // Set of all consumers from {connection} consuming from {queue}
	connectionQueueUnackedTemplate   = "rmq::connection::{connection}::queue::{queue}::unacked"   // List of deliveries consumers of {connection} are currently consuming

	queuesKey             = "rmq::queues"                   // Set of all open queues
	queueReadyTemplate    = "rmq::queue::{queue}::ready"    // List of deliveries in that {queue} (right is first and oldest, left is last and youngest)
	queueRejectedTemplate = "rmq::queue::{queue}::rejected" // List of rejected deliveries from that {queue}

	phConnection = "{connection}" // connection name
	phQueue      = "{queue}"      // queue name
	phConsumer   = "{consumer}"   // consumer name (consisting of tag and token)
)

// PublishQueue is an interface that can be used to test publishing
type Queue interface {
	Publish(payload string) bool
	StartConsuming(prefetchLimit int) bool
	AddConsumer(tag string, consumer Consumer) string
	ReturnAllRejectedDeliveries() int
	PurgeReady() bool
	PurgeRejected() bool
}

type redisQueue struct {
	name             string
	connectionName   string
	queuesKey        string // key to list of queues consumed by this connection
	consumersKey     string // key to set of consumers using this connection
	readyKey         string // key to list of ready deliveries
	rejectedKey      string // key to list of rejected deliveries
	unackedKey       string // key to list of currently consuming deliveries
	redisClient      *redis.Client
	deliveryChan     chan Delivery // nil for publish channels, not nil for consuming channels
	prefetchLimit    int           // max number of prefetched deliveries number of unacked can go up to prefetchLimit + numConsumers
	consumingStopped bool
}

func newQueue(name, connectionName, queuesKey string, redisClient *redis.Client) *redisQueue {
	consumersKey := strings.Replace(connectionQueueConsumersTemplate, phConnection, connectionName, 1)
	consumersKey = strings.Replace(consumersKey, phQueue, name, 1)

	readyKey := strings.Replace(queueReadyTemplate, phQueue, name, 1)
	rejectedKey := strings.Replace(queueRejectedTemplate, phQueue, name, 1)

	unackedKey := strings.Replace(connectionQueueUnackedTemplate, phConnection, connectionName, 1)
	unackedKey = strings.Replace(unackedKey, phQueue, name, 1)

	queue := &redisQueue{
		name:           name,
		connectionName: connectionName,
		queuesKey:      queuesKey,
		consumersKey:   consumersKey,
		readyKey:       readyKey,
		rejectedKey:    rejectedKey,
		unackedKey:     unackedKey,
		redisClient:    redisClient,
	}
	return queue
}

func (queue *redisQueue) String() string {
	return fmt.Sprintf("[%s conn:%s]", queue.name, queue.connectionName)
}

// Publish adds a delivery with the given payload to the queue
func (queue *redisQueue) Publish(payload string) bool {
	// debug(fmt.Sprintf("publish %s %s", payload, queue)) // COMMENTOUT
	return !redisErrIsNil(queue.redisClient.LPush(queue.readyKey, payload))
}

// PurgeReady removes all ready deliveries from the queue and returns the number of purged deliveries
func (queue *redisQueue) PurgeReady() bool {
	result := queue.redisClient.Del(queue.readyKey)
	if redisErrIsNil(result) {
		return false
	}
	return result.Val() > 0
}

// PurgeRejected removes all rejected deliveries from the queue and returns the number of purged deliveries
func (queue *redisQueue) PurgeRejected() bool {
	result := queue.redisClient.Del(queue.rejectedKey)
	if redisErrIsNil(result) {
		return false
	}
	return result.Val() > 0
}

func (queue *redisQueue) ReadyCount() int {
	result := queue.redisClient.LLen(queue.readyKey)
	if redisErrIsNil(result) {
		return 0
	}
	return int(result.Val())
}

func (queue *redisQueue) UnackedCount() int {
	result := queue.redisClient.LLen(queue.unackedKey)
	if redisErrIsNil(result) {
		return 0
	}
	return int(result.Val())
}

func (queue *redisQueue) RejectedCount() int {
	result := queue.redisClient.LLen(queue.rejectedKey)
	if redisErrIsNil(result) {
		return 0
	}
	return int(result.Val())
}

// ReturnAllUnackedDeliveries moves all unacked deliveries back to the ready
// queue and deletes the unacked key afterwards, returns number of returned
// deliveries
func (queue *redisQueue) ReturnAllUnackedDeliveries() int {
	result := queue.redisClient.LLen(queue.unackedKey)
	if redisErrIsNil(result) {
		return 0
	}

	unackedCount := int(result.Val())
	for i := 0; i < unackedCount; i++ {
		if redisErrIsNil(queue.redisClient.RPopLPush(queue.unackedKey, queue.readyKey)) {
			return i
		}
		// debug(fmt.Sprintf("queue returned unacked delivery %s %s", result.Val(), queue.readyKey)) // COMMENTOUT
	}

	return unackedCount
}

// ReturnAllRejectedDeliveries moves all rejected deliveries back to the ready
// list and returns the number of returned deliveries
func (queue *redisQueue) ReturnAllRejectedDeliveries() int {
	result := queue.redisClient.LLen(queue.rejectedKey)
	if redisErrIsNil(result) {
		return 0
	}

	rejectedCount := int(result.Val())
	return queue.ReturnRejectedDeliveries(rejectedCount)
}

// ReturnRejectedDeliveries tries to return count rejected deliveries back to
// the ready list and returns the number of returned deliveries
func (queue *redisQueue) ReturnRejectedDeliveries(count int) int {
	if count == 0 {
		return 0
	}

	for i := 0; i < count; i++ {
		if redisErrIsNil(queue.redisClient.RPopLPush(queue.rejectedKey, queue.readyKey)) {
			return i
		}
		// debug(fmt.Sprintf("queue returned rejected delivery %s %s", result.Val(), queue.readyKey)) // COMMENTOUT
	}

	return count
}

// CloseInConnection closes the queue in the associated connection by removing all related keys
func (queue *redisQueue) CloseInConnection() {
	redisErrIsNil(queue.redisClient.Del(queue.unackedKey))
	redisErrIsNil(queue.redisClient.Del(queue.consumersKey))
	redisErrIsNil(queue.redisClient.SRem(queue.queuesKey, queue.name))
}

// StartConsuming starts consuming into a channel of size prefetchLimit
// must be called before consumers can be added!
func (queue *redisQueue) StartConsuming(prefetchLimit int) bool {
	if queue.deliveryChan != nil {
		return false
	}

	// add queue to list of queues consumed on this connection
	if redisErrIsNil(queue.redisClient.SAdd(queue.queuesKey, queue.name)) {
		log.Panicf("queue failed to start consuming %s", queue)
	}

	queue.prefetchLimit = prefetchLimit
	queue.deliveryChan = make(chan Delivery, prefetchLimit)
	log.Printf("queue started consuming %s %d", queue, prefetchLimit)
	go queue.consume()
	return true
}

func (queue *redisQueue) StopConsuming() {
	queue.consumingStopped = true
}

// AddConsumer adds a consumer to the queue and returns its internal name
// panics if StartConsuming wasn't called before!
func (queue *redisQueue) AddConsumer(tag string, consumer Consumer) string {
	if queue.deliveryChan == nil {
		log.Panicf("queue failed to add consumer, call StartConsuming first! %s", queue)
	}

	name := fmt.Sprintf("%s-%s", tag, uniuri.NewLen(6))

	// add consumer to list of consumers of this queue
	if redisErrIsNil(queue.redisClient.SAdd(queue.consumersKey, name)) {
		log.Panicf("queue failed to add consumer %s %s", queue, consumer)
	}

	go queue.addConsumer(consumer)
	log.Printf("queue added consumer %s %s", queue, name)
	return name
}

func (queue *redisQueue) GetConsumers() []string {
	result := queue.redisClient.SMembers(queue.consumersKey)
	if redisErrIsNil(result) {
		return []string{}
	}
	return result.Val()
}

func (queue *redisQueue) RemoveConsumer(name string) bool {
	result := queue.redisClient.SRem(queue.consumersKey, name)
	if redisErrIsNil(result) {
		return false
	}
	return result.Val() > 0
}

func (queue *redisQueue) RemoveAllConsumers() int {
	result := queue.redisClient.Del(queue.consumersKey)
	if redisErrIsNil(result) {
		return 0
	}
	return int(result.Val())
}

func (queue *redisQueue) consume() {
	for {
		batchSize := queue.batchSize()
		wantMore := queue.consumeBatch(batchSize)

		if !wantMore {
			time.Sleep(time.Millisecond)
		}

		if queue.consumingStopped {
			log.Printf("queue stopped consuming %s", queue)
			return
		}
	}
}

func (queue *redisQueue) batchSize() int {
	prefetchCount := len(queue.deliveryChan)
	prefetchLimit := queue.prefetchLimit - prefetchCount
	// TODO: ignore ready count here and just return prefetchLimit?
	if readyCount := queue.ReadyCount(); readyCount < prefetchLimit {
		return readyCount
	}
	return prefetchLimit
}

// consumeBatch tries to read batchSize deliveries, returns true if any and all were consumed
func (queue *redisQueue) consumeBatch(batchSize int) bool {
	if batchSize == 0 {
		return false
	}

	for i := 0; i < batchSize; i++ {
		result := queue.redisClient.RPopLPush(queue.readyKey, queue.unackedKey)
		if redisErrIsNil(result) {
			// debug(fmt.Sprintf("queue consumed last batch %s %d", queue, i)) // COMMENTOUT
			return false
		}

		// debug(fmt.Sprintf("consume %d/%d %s %s", i, batchSize, result.Val(), queue)) // COMMENTOUT
		queue.deliveryChan <- newDelivery(result.Val(), queue.unackedKey, queue.rejectedKey, queue.redisClient)
	}

	// debug(fmt.Sprintf("queue consumed batch %s %d", queue, batchSize)) // COMMENTOUT
	return true
}

func (queue *redisQueue) addConsumer(consumer Consumer) {
	for delivery := range queue.deliveryChan {
		// debug(fmt.Sprintf("consumer consume %s %s", delivery, consumer)) // COMMENTOUT
		consumer.Consume(delivery)
	}
}

// redisErrIsNil returns false if there is no error, true if the result error is nil and panics if there's another error
func redisErrIsNil(result redis.Cmder) bool {
	switch result.Err() {
	case nil:
		return false
	case redis.Nil:
		return true
	default:
		log.Panicf("redis error is not nil %s", result.Err())
		return false
	}
}

func debug(message string) {
	// log.Printf("queue.debug: %s", message) // COMMENTOUT
}
