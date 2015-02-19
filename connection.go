package queue

import (
	"fmt"
	"log"
	"strings"
	"time"

	redis "github.com/adjust/redis-latest-head" // TODO: update
	"github.com/adjust/uniuri"
)

// Connection is the entry point. Use a connection to access queues, consumers and deliveries
// Each connection has a single heartbeat shared among all consumers
type Connection struct {
	Name             string
	heartbeatKey     string // key to keep alive
	queuesKey        string // key to list of queues consumed by this connection
	redisClient      *redis.Client
	heartbeatStopped bool
}

// OpenConnection opens and returns a new connection
func OpenConnection(tag, host, port string, db int) *Connection {
	redisClient := redis.NewTCPClient(&redis.Options{
		Addr: host + ":" + port,
		DB:   int64(db)},
	)

	name := fmt.Sprintf("%s-%s", tag, uniuri.NewLen(6))
	redisClient.SAdd(connectionsKey, name)

	connection := &Connection{
		Name:         name,
		heartbeatKey: strings.Replace(connectionHeartbeatTemplate, phConnection, name, 1),
		queuesKey:    strings.Replace(connectionQueuesTemplate, phConnection, name, 1),
		redisClient:  redisClient,
	}

	go connection.heartbeat()
	log.Printf("queue connection connected to %s %s:%s %d", name, host, port, db)
	return connection
}

func (connection *Connection) String() string {
	return connection.Name
}

// GetConnections returns a list of all open connections
func (connection *Connection) GetConnections() []string {
	result := connection.redisClient.SMembers(connectionsKey)
	if result.Err() != nil {
		log.Printf("queue connection failed to get connections %s", result.Err())
		return []string{}
	}
	return result.Val()
}

// Check retuns true if the connection is currently active in terms of heartbeat
func (connection *Connection) Check() bool {
	heartbeatKey := strings.Replace(connectionHeartbeatTemplate, phConnection, connection.Name, 1)
	result := connection.redisClient.TTL(heartbeatKey)
	if result.Err() != nil {
		return false
	}
	return result.Val() > 0
}

// StopHeartbeat stopps the heartbeat of the connection
// it does not remove it from the list of connections so it can later be found by the cleaner
func (connection *Connection) StopHeartbeat() error {
	connection.heartbeatStopped = true
	return connection.redisClient.Del(connection.heartbeatKey).Err()
}

func (connection *Connection) Close() error {
	return connection.redisClient.SRem(connectionsKey, connection.Name).Err()
}

// OpenQueue opens and returns the queue with a given name
func (connection *Connection) OpenQueue(name string) *Queue {
	result := connection.redisClient.SAdd(queuesKey, name)
	if result.Err() != nil {
		log.Printf("queue connection failed to open queue %s %s", name, result.Err())
	}
	queue := newQueue(name, connection.Name, connection.queuesKey, connection.redisClient)
	return queue
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
		log.Printf("queue connection failed to close queue %s %s", name, result.Err())
		return false
	}
	return result.Val() > 0
}

// CloseAllQueues clears the list of open queues
func (connection *Connection) CloseAllQueues() int {
	result := connection.redisClient.Del(queuesKey)
	if result.Err() != nil {
		log.Printf("queue connection failed to close all queue %s", result.Err())
		return 0
	}
	return int(result.Val())
}

// GetConsumingQueues returns a list of all queues consumed by this connection
func (connection *Connection) GetConsumingQueues() []string {
	result := connection.redisClient.SMembers(connection.queuesKey)
	if result.Err() != nil {
		log.Printf("queue connection failed to get open queues %s", result.Err())
		return []string{}
	}
	return result.Val()
}

// heartbeat keeps the heartbeat key alive
func (connection *Connection) heartbeat() {
	for {
		connection.redisClient.SetEx(connection.heartbeatKey, 3*time.Second, "1")
		time.Sleep(time.Second)

		if connection.heartbeatStopped {
			log.Printf("queue connection stopped heartbeat %s", connection)
			return
		}
	}
}

// hijackConnection reopens an existing connection for inspection purposes without starting a heartbeat
func (connection *Connection) hijackConnection(name string) *Connection {
	return &Connection{
		Name:         name,
		heartbeatKey: strings.Replace(connectionHeartbeatTemplate, phConnection, name, 1),
		queuesKey:    strings.Replace(connectionQueuesTemplate, phConnection, name, 1),
		redisClient:  connection.redisClient,
	}
}

// openQueue opens a queue without adding it to the set of queues
func (connection *Connection) openQueue(name string) *Queue {
	return newQueue(name, connection.Name, connection.queuesKey, connection.redisClient)
}
