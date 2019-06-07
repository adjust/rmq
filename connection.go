package rmq

import (
	"fmt"
	"strings"
	"time"

	"github.com/adjust/uniuri"
	"github.com/go-redis/redis/v7"
)

const heartbeatDuration = time.Minute

// Connection is an interface that can be used to test publishing
type Connection interface {
	OpenQueue(name string) Queue
	CollectStats(queueList []string) (Stats, error)
	GetOpenQueues() ([]string, error)
}

// Connection is the entry point. Use a connection to access queues, consumers and deliveries
// Each connection has a single heartbeat shared among all consumers
type redisConnection struct {
	Name             string
	heartbeatKey     string // key to keep alive
	queuesKey        string // key to list of queues consumed by this connection
	redisClient      RedisClient
	heartbeatStopped bool
}

// OpenConnectionWithRedisClient opens and returns a new connection
func OpenConnectionWithRedisClient(tag string, redisClient *redis.Client) (*redisConnection, error) {
	return openConnectionWithRedisClient(tag, RedisWrapper{redisClient})
}

// OpenConnectionWithTestRedisClient opens and returns a new connection which
// uses a test redis client internally. This is useful in integration tests.
func OpenConnectionWithTestRedisClient(tag string) (*redisConnection, error) {
	return openConnectionWithRedisClient(tag, NewTestRedisClient())
}

func openConnectionWithRedisClient(tag string, redisClient RedisClient) (*redisConnection, error) {
	name := fmt.Sprintf("%s-%s", tag, uniuri.NewLen(6))

	connection := &redisConnection{
		Name:         name,
		heartbeatKey: strings.Replace(connectionHeartbeatTemplate, phConnection, name, 1),
		queuesKey:    strings.Replace(connectionQueuesTemplate, phConnection, name, 1),
		redisClient:  redisClient,
	}

	ok, err := connection.updateHeartbeat()
	// TODO: do it like this in more places?
	if !ok || err != nil { // checks the connection
		return nil, err
	}

	// add to connection set after setting heartbeat to avoid race with cleaner
	ok, err = redisClient.SAdd(connectionsKey, name)
	if !ok {
		return nil, err
	}

	go connection.heartbeat()
	// log.Printf("rmq connection connected to %s %s:%s %d", name, network, address, db)
	return connection, nil
}

// OpenConnection opens and returns a new connection
func OpenConnection(tag, network, address string, db int) (*redisConnection, error) {
	redisClient := redis.NewClient(&redis.Options{
		Network: network,
		Addr:    address,
		DB:      db,
	})
	return OpenConnectionWithRedisClient(tag, redisClient)
}

// OpenQueue opens and returns the queue with a given name
func (connection *redisConnection) OpenQueue(name string) Queue {
	connection.redisClient.SAdd(queuesKey, name)
	queue := newQueue(name, connection.Name, connection.queuesKey, connection.redisClient)
	return queue
}

func (connection *redisConnection) CollectStats(queueList []string) (Stats, error) {
	return CollectStats(queueList, connection)
}

func (connection *redisConnection) String() string {
	return connection.Name
}

// GetConnections returns a list of all open connections
func (connection *redisConnection) GetConnections() ([]string, error) {
	return connection.redisClient.SMembers(connectionsKey)
}

// Check retuns true if the connection is currently active in terms of heartbeat
func (connection *redisConnection) Check() (bool, error) {
	heartbeatKey := strings.Replace(connectionHeartbeatTemplate, phConnection, connection.Name, 1)
	ttl, _, err := connection.redisClient.TTL(heartbeatKey)
	return ttl > 0, err
}

// StopHeartbeat stops the heartbeat of the connection
// it does not remove it from the list of connections so it can later be found by the cleaner
func (connection *redisConnection) StopHeartbeat() (bool, error) {
	connection.heartbeatStopped = true
	_, ok, err := connection.redisClient.Del(connection.heartbeatKey)
	return ok, err
}

func (connection *redisConnection) Close() (bool, error) {
	_, ok, err := connection.redisClient.SRem(connectionsKey, connection.Name)
	return ok, err
}

// GetOpenQueues returns a list of all open queues
func (connection *redisConnection) GetOpenQueues() ([]string, error) {
	return connection.redisClient.SMembers(queuesKey)
}

// CloseAllQueues closes all queues by removing them from the global list
func (connection *redisConnection) CloseAllQueues() (int, error) {
	count, _, err := connection.redisClient.Del(queuesKey)
	return count, err
}

// CloseAllQueuesInConnection closes all queues in the associated connection by removing all related keys
func (connection *redisConnection) CloseAllQueuesInConnection() error {
	connection.redisClient.Del(connection.queuesKey)
	// debug(fmt.Sprintf("connection closed all queues %s %d", connection, connection.queuesKey)) // COMMENTOUT
	return nil
}

// GetConsumingQueues returns a list of all queues consumed by this connection
func (connection *redisConnection) GetConsumingQueues() ([]string, error) {
	return connection.redisClient.SMembers(connection.queuesKey)
}

// heartbeat keeps the heartbeat key alive
func (connection *redisConnection) heartbeat() {
	for {
		ok, err := connection.updateHeartbeat()
		if err != nil {
			// TODO: what to do here???
			// one idea was to wait a bit and retry, but make sure the key wasn't expired in between
			// if it was, do panic
		}

		if !ok {
			// log.Printf("rmq connection failed to update heartbeat %s", connection)
		}

		time.Sleep(time.Second)

		if connection.heartbeatStopped {
			// log.Printf("rmq connection stopped heartbeat %s", connection)
			return
		}
	}
}

func (connection *redisConnection) updateHeartbeat() (bool, error) {
	return connection.redisClient.Set(connection.heartbeatKey, "1", heartbeatDuration)
}

// hijackConnection reopens an existing connection for inspection purposes without starting a heartbeat
func (connection *redisConnection) hijackConnection(name string) *redisConnection {
	return &redisConnection{
		Name:         name,
		heartbeatKey: strings.Replace(connectionHeartbeatTemplate, phConnection, name, 1),
		queuesKey:    strings.Replace(connectionQueuesTemplate, phConnection, name, 1),
		redisClient:  connection.redisClient,
	}
}

// openQueue opens a queue without adding it to the set of queues
func (connection *redisConnection) openQueue(name string) *redisQueue {
	return newQueue(name, connection.Name, connection.queuesKey, connection.redisClient)
}

// flushDb flushes the redis database to reset everything, used in tests
func (connection *redisConnection) flushDb() {
	connection.redisClient.FlushDb()
}
