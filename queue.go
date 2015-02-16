package queue

import (
	"log"
	"strings"

	"github.com/adjust/redis"
)

const (
	queueKeyTemplate = "rmq::queue::{name}::packages" // List of packages in that queue (right is first and oldest, left is last and youngest)

	phName = "{name}"
)

type Queue struct {
	name        string
	queueKey    string
	redisClient *redis.Client
}

func newQueue(name string, redisClient *redis.Client) *Queue {
	queue := &Queue{
		name:        name,
		queueKey:    strings.Replace(queueKeyTemplate, phName, name, -1),
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
