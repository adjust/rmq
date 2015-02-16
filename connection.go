package queue

import (
	"log"

	"github.com/adjust/redis"
)

const queuesKey = "rmq::queues" // Set of all open queues

type Connection struct {
	redisClient *redis.Client
}

func NewConnection(host, port string, db int) *Connection {
	return &Connection{
		redisClient: redis.NewTCPClient(host+":"+port, "", int64(db)),
	}
}

// OpenQueue opens and returns the Queue with a given name
func (connection *Connection) OpenQueue(name string) *Queue {
	result := connection.redisClient.SAdd(queuesKey, name)
	if result.Err() != nil {
		log.Printf("queue connection failed to open queue %s %s", name, result.Err())
	}
	return newQueue(name, connection.redisClient)
}

// GetOpenQueues returns a list of all open queues
func (connection *Connection) GetOpenQueues() []string {
	result := connection.redisClient.SMembers(queuesKey)
	if result.Err() != nil {
		log.Printf("queue connection failed to get open queues %s", result.Err())
		return []string{}
	}
	return result.Val()
}

// CloseQueue removes a queue from the list of open queues
func (connection *Connection) CloseQueue(name string) bool {
	result := connection.redisClient.SRem(queuesKey, name)
	if result.Err() != nil {
		return false
	}
	return result.Val() > 0
}

// CloseAllQueues clears the list of open queues
func (connection *Connection) CloseAllQueues() int {
	result := connection.redisClient.Del(queuesKey)
	if result.Err() != nil {
		return 0
	}
	return int(result.Val())
}
