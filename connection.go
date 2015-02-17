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
	Name         string
	heartbeatKey string
	redisClient  *redis.Client
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
		redisClient:  redisClient,
	}

	go connection.heartbeat()
	log.Printf("queue connection connected to %s %s:%s %d", name, host, port, db)
	return connection
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

// CheckConnection retuns true if the named connection is currently active in terms of heartbeat
func (connection *Connection) CheckConnection(name string) bool {
	heartbeatKey := strings.Replace(connectionHeartbeatTemplate, phConnection, name, 1)
	result := connection.redisClient.TTL(heartbeatKey)
	if result.Err() != nil {
		return false
	}
	return result.Val() > 0
}

// CloseConnection removes the connection with the given name from the connections set
func (connection *Connection) CloseConnection(name string) bool {
	result := connection.redisClient.SRem(connectionsKey, name)
	if result.Err() != nil {
		log.Printf("queue connection failed to close connection %s %s", name, result.Err())
		return false
	}
	return result.Val() > 0
}

// CloseAllConnections removes all connections from the connection set
func (connection *Connection) CloseAllConnections() int {
	result := connection.redisClient.Del(connectionsKey)
	if result.Err() != nil {
		log.Printf("queue connection failed to close all consumer %s", result.Err())
		return 0
	}
	return int(result.Val())
}

// OpenQueue opens and returns the Queue with a given name
func (connection *Connection) OpenQueue(name string) *Queue {
	result := connection.redisClient.SAdd(queuesKey, name)
	if result.Err() != nil {
		log.Printf("queue connection failed to open queue %s %s", name, result.Err())
	}
	return newQueue(name, connection.Name, connection.redisClient)
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

// heartbeat keeps the heartbeat key alive
func (connection *Connection) heartbeat() {
	for {
		connection.redisClient.SetEx(connection.heartbeatKey, 3*time.Second, "1")
		time.Sleep(time.Second)
	}
}
