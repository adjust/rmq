package queue

import (
	"strings"

	"github.com/adjust/redis"
)

const (
	queueKeyTemplate = "rmq::queue::{name}::packages" // List of packages in that queue

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
