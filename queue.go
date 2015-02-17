package queue

import (
	"fmt"
	"log"
	"strings"

	redis "github.com/adjust/redis-latest-head" // TODO: update
	"github.com/adjust/uniuri"
)

const (
	connectionsKey                 = "rmq::connections"                                       // Set of connection names
	connectionHeartbeatTemplate    = "rmq::connection::{connection}::heartbeat"               // expires after {connection} died
	connectionQueuesTemplate       = "rmq::connection::{connection}::queues"                  // Set of queues consumers of {connection} are consuming
	connectionQueueUnackedTemplate = "rmq::connection::{connection}::queue::{queue}::unacked" // List of deliveries consumers of {connection} are currently consuming

	queuesKey          = "rmq::queues"                // Set of all open queues
	queueReadyTemplate = "rmq::queue::{queue}::ready" // List of deliveries in that {queue} (right is first and oldest, left is last and youngest)

	consumersKey          = "rmq::consumers"                   // Set of all consumers
	consumerQueueTemplate = "rmq::consumer::{consumer}::queue" // queue name that {consumer} is consuming from

	phConnection = "{connection}" // connection name
	phQueue      = "{queue}"      // queue name
	phConsumer   = "{consumer}"   // consumer name (consisting of tag and token)

	consumeChannelSize = 10 // TODO: make a setting? increase?
)

type Queue struct {
	name           string
	connectionName string // TODO: remove?
	readyKey       string // key to list of ready deliveries
	unackedKey     string // key to list of currently consuming deliveries
	redisClient    *redis.Client
	deliveryChan   chan Delivery // nil for publish channels, not nil for consuming channels
}

func newQueue(name, connectionName string, redisClient *redis.Client) *Queue {
	unackedKey := connectionQueueUnackedTemplate
	unackedKey = strings.Replace(unackedKey, phConnection, connectionName, -1)
	unackedKey = strings.Replace(unackedKey, phQueue, name, -1)

	queue := &Queue{
		name:           name,
		connectionName: connectionName,
		readyKey:       strings.Replace(queueReadyTemplate, phQueue, name, -1),
		unackedKey:     unackedKey,
		redisClient:    redisClient,
	}
	return queue
}

func (queue *Queue) Publish(payload string) error {
	return queue.redisClient.LPush(queue.readyKey, payload).Err()
}

func (queue *Queue) ReadyCount() int {
	result := queue.redisClient.LLen(queue.readyKey)
	if result.Err() != nil {
		log.Printf("queue failed to get ready count %s %s", queue, result.Err())
		return 0
	}
	return int(result.Val())
}

func (queue *Queue) UnackedCount() int {
	result := queue.redisClient.LLen(queue.unackedKey)
	if result.Err() != nil {
		log.Printf("queue failed to get unacked count %s %s", queue, result.Err())
		return 0
	}
	return int(result.Val())
}

func (queue *Queue) Clear() int {
	result := queue.redisClient.Del(queue.readyKey)
	if result.Err() != nil {
		log.Printf("queue failed to clear %s %s", queue, result.Err())
		return 0
	}
	return int(result.Val())
}

// AddConsumer adds a consumer to the queue and returns its internal name
func (queue *Queue) AddConsumer(tag string, consumer Consumer) string {
	name := fmt.Sprintf("%s-%s", tag, uniuri.NewLen(6))
	result := queue.redisClient.SAdd(consumersKey, name)
	if result.Err() != nil {
		log.Printf("queue failed to add consumer %s %s", name, result.Err())
	}

	queue.startConsuming()
	go queue.addConsumer(consumer)
	log.Printf("queue added consumer %s %s", queue, name)
	return name
}

func (queue *Queue) GetConsumers() []string {
	result := queue.redisClient.SMembers(consumersKey)
	if result.Err() != nil {
		log.Printf("queue failed to get consumers %s", result.Err())
		return []string{}
	}
	return result.Val()
}

func (queue *Queue) RemoveConsumer(name string) bool {
	result := queue.redisClient.SRem(consumersKey, name)
	if result.Err() != nil {
		log.Printf("queue failed to remove consumer %s %s %s", queue, name, result.Err())
		return false
	}
	return result.Val() > 0
}

func (queue *Queue) RemoveAllConsumers() int {
	result := queue.redisClient.Del(consumersKey)
	if result.Err() != nil {
		log.Printf("queue failed to remove all consumers %s %s", queue, result.Err())
		return 0
	}
	return int(result.Val())
}

func (queue *Queue) startConsuming() {
	if queue.deliveryChan != nil {
		return
	}
	queue.deliveryChan = make(chan Delivery, consumeChannelSize)
	queuesKey := strings.Replace(connectionQueuesTemplate, phConnection, queue.connectionName, -1)
	queue.redisClient.LPush(queuesKey, queue.name)
	go queue.consume()
}

func (queue *Queue) consume() {
	for {
		result := queue.redisClient.BRPopLPush(queue.readyKey, queue.unackedKey, 0)
		if result.Err() != nil {
			log.Printf("queue failed to consume %s %s", queue, result.Err())
			continue
		}
		queue.deliveryChan <- newDelivery(result.Val())
	}
}

func (queue *Queue) addConsumer(consumer Consumer) {
	for delivery := range queue.deliveryChan {
		consumer.Consume(delivery)
	}
}
