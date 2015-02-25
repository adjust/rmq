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
		DB:   int64(db),
	})

	name := fmt.Sprintf("%s-%s", tag, uniuri.NewLen(6))
	redisErrIsNil(redisClient.SAdd(connectionsKey, name))

	connection := &Connection{
		Name:         name,
		heartbeatKey: strings.Replace(connectionHeartbeatTemplate, phConnection, name, 1),
		queuesKey:    strings.Replace(connectionQueuesTemplate, phConnection, name, 1),
		redisClient:  redisClient,
	}

	if !connection.updateHeartbeat() {
		log.Panicf("queue connection failed to update heartbeat %s", connection)
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
	if redisErrIsNil(result) {
		return []string{}
	}
	return result.Val()
}

// Check retuns true if the connection is currently active in terms of heartbeat
func (connection *Connection) Check() bool {
	heartbeatKey := strings.Replace(connectionHeartbeatTemplate, phConnection, connection.Name, 1)
	result := connection.redisClient.TTL(heartbeatKey)
	if redisErrIsNil(result) {
		return false
	}
	return result.Val() > 0
}

// StopHeartbeat stops the heartbeat of the connection
// it does not remove it from the list of connections so it can later be found by the cleaner
func (connection *Connection) StopHeartbeat() bool {
	connection.heartbeatStopped = true
	return !redisErrIsNil(connection.redisClient.Del(connection.heartbeatKey))
}

func (connection *Connection) Close() bool {
	return !redisErrIsNil(connection.redisClient.SRem(connectionsKey, connection.Name))
}

// OpenQueue opens and returns the queue with a given name
func (connection *Connection) OpenQueue(name string) *Queue {
	redisErrIsNil(connection.redisClient.SAdd(queuesKey, name))
	queue := newQueue(name, connection.Name, connection.queuesKey, connection.redisClient)
	return queue
}

// GetOpenQueues returns a list of all open queues
func (connection *Connection) GetOpenQueues() []string {
	result := connection.redisClient.SMembers(queuesKey)
	if redisErrIsNil(result) {
		return []string{}
	}
	return result.Val()
}

// CloseAllQueues closes all queues by removing them from the global list
func (connection *Connection) CloseAllQueues() int {
	result := connection.redisClient.Del(queuesKey)
	if redisErrIsNil(result) {
		return 0
	}
	return int(result.Val())
}

// CloseAllQueuesInConnection closes all queues in the associated connection by removing all related keys
func (connection *Connection) CloseAllQueuesInConnection() error {
	redisErrIsNil(connection.redisClient.Del(connection.queuesKey))
	// debug(fmt.Sprintf("connection closed all queues %s %d", connection, connection.queuesKey)) // COMMENTOUT
	return nil
}

// GetConsumingQueues returns a list of all queues consumed by this connection
func (connection *Connection) GetConsumingQueues() []string {
	result := connection.redisClient.SMembers(connection.queuesKey)
	if redisErrIsNil(result) {
		return []string{}
	}
	return result.Val()
}

// heartbeat keeps the heartbeat key alive
func (connection *Connection) heartbeat() {
	for {
		if !connection.updateHeartbeat() {
			log.Printf("redis connection failed to update heartbeat %s", connection)
		}

		time.Sleep(time.Second)

		if connection.heartbeatStopped {
			log.Printf("queue connection stopped heartbeat %s", connection)
			return
		}
	}
}

func (connection *Connection) updateHeartbeat() bool {
	return !redisErrIsNil(connection.redisClient.SetEx(connection.heartbeatKey, 3*time.Second, "1"))
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

// flushDb flushes the redis database to reset everything, used in tests
func (connection *Connection) flushDb() {
	connection.redisClient.FlushDb()
}
