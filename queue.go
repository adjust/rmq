package queue

import (
	"fmt"
	"log"
	"strings"

	"github.com/adjust/redis"
	"github.com/adjust/uniuri"
)

const (
	queueKeyTemplate    = "rmq::queue::{queue}::deliveries" // List of deliveries in that queue (right is first and oldest, left is last and youngest)
	consumerKeyTemplate = "rmq::queue::{queue}::consumers"  // Set of consumer names

	consumersKey          = "rmq::consumers"                       // Set of all consumers
	consumersHeartbeatKey = "rmq::consumer::{consumer}::heartbeat" // expires after consumer became died

	phQueue    = "{queue}"    // queue name
	phConsumer = "{consumer}" // consumer name (consisting of tag and token)
)

type Queue struct {
	name        string
	queueKey    string
	redisClient *redis.Client
}

func newQueue(name string, redisClient *redis.Client) *Queue {
	queue := &Queue{
		name:        name,
		queueKey:    strings.Replace(queueKeyTemplate, phQueue, name, -1),
		redisClient: redisClient,
	}
	return queue
}

func (queue *Queue) Publish(payload string) error {
	return queue.redisClient.LPush(queue.queueKey, payload).Err()
}

func (queue *Queue) Length() int {
	result := queue.redisClient.LLen(queue.queueKey)
	if result.Err() != nil {
		log.Printf("queue failed to get length %s %s", queue, result.Err())
		return 0
	}
	return int(result.Val())
}

func (queue *Queue) Clear() int {
	result := queue.redisClient.Del(queue.queueKey)
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
