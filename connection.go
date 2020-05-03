package rmq

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/adjust/uniuri"
	"github.com/go-redis/redis/v7"
)

// entitify being connection/queue/delivery
var ErrorNotFound = errors.New("entity not found")

const heartbeatDuration = time.Minute

// Connection is an interface that can be used to test publishing
type Connection interface {
	OpenQueue(name string) (Queue, error)
	CollectStats(queueList []string) (Stats, error)
	GetOpenQueues() ([]string, error)

	// internals
	// used in cleaner
	check() error
	getConnections() ([]string, error)
	hijackConnection(name string) Connection
	closeStaleConnection() error
	getConsumingQueues() ([]string, error)
	// used for stats
	openQueue(name string) Queue
	// used in tests
	stopHeartbeat() error
	flushDb() error
	unlistAllQueues() error
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

	if err := connection.updateHeartbeat(); err != nil { // checks the connection
		return nil, err
	}

	// add to connection set after setting heartbeat to avoid race with cleaner
	if _, err := redisClient.SAdd(connectionsKey, name); err != nil {
		return nil, err
	}

	go connection.heartbeat()
	// log.Printf("rmq connection connected to %s %s:%s %d", name, network, address, db)
	return connection, nil
}

// OpenConnection opens and returns a new connection
func OpenConnection(tag, network, address string, db int) (Connection, error) {
	redisClient := redis.NewClient(&redis.Options{
		Network: network,
		Addr:    address,
		DB:      db,
	})
	return OpenConnectionWithRedisClient(tag, redisClient)
}

// OpenQueue opens and returns the queue with a given name
func (connection *redisConnection) OpenQueue(name string) (Queue, error) {
	if _, err := connection.redisClient.SAdd(queuesKey, name); err != nil {
		return nil, err
	}

	return connection.openQueue(name), nil
}

func (connection *redisConnection) CollectStats(queueList []string) (Stats, error) {
	return CollectStats(queueList, connection)
}

func (connection *redisConnection) String() string {
	return connection.Name
}

// getConnections returns a list of all open connections
func (connection *redisConnection) getConnections() ([]string, error) {
	return connection.redisClient.SMembers(connectionsKey)
}

// check retuns true if the connection is currently active in terms of heartbeat
func (connection *redisConnection) check() error {
	heartbeatKey := strings.Replace(connectionHeartbeatTemplate, phConnection, connection.Name, 1)
	ttl, err := connection.redisClient.TTL(heartbeatKey)
	if err != nil {
		return err
	}
	if ttl <= 0 {
		return ErrorNotFound
	}
	return nil
}

// stopHeartbeat stops the heartbeat of the connection
// it does not remove it from the list of connections so it can later be found by the cleaner
func (connection *redisConnection) stopHeartbeat() error {
	// TODO: use atomic?
	connection.heartbeatStopped = true
	_, err := connection.redisClient.Del(connection.heartbeatKey)
	return err
}

// GetOpenQueues returns a list of all open queues
func (connection *redisConnection) GetOpenQueues() ([]string, error) {
	return connection.redisClient.SMembers(queuesKey)
}

// unlistAllQueues closes all queues by removing them from the global list
func (connection *redisConnection) unlistAllQueues() error {
	_, err := connection.redisClient.Del(queuesKey)
	return err
}

// getConsumingQueues returns a list of all queues consumed by this connection
func (connection *redisConnection) getConsumingQueues() ([]string, error) {
	return connection.redisClient.SMembers(connection.queuesKey)
}

// heartbeat keeps the heartbeat key alive
func (connection *redisConnection) heartbeat() {
	for {
		if err := connection.updateHeartbeat(); err != nil {
			// TODO: what to do here???
			// one idea was to wait a bit and retry, but make sure the key wasn't expired in between
			// if it was, do panic
		}

		time.Sleep(time.Second)

		if connection.heartbeatStopped {
			// log.Printf("rmq connection stopped heartbeat %s", connection)
			return
		}
	}
}

func (connection *redisConnection) updateHeartbeat() error {
	return connection.redisClient.Set(connection.heartbeatKey, "1", heartbeatDuration)
}

// hijackConnection reopens an existing connection for inspection purposes without starting a heartbeat
func (connection *redisConnection) hijackConnection(name string) Connection {
	return &redisConnection{
		Name:         name,
		heartbeatKey: strings.Replace(connectionHeartbeatTemplate, phConnection, name, 1),
		queuesKey:    strings.Replace(connectionQueuesTemplate, phConnection, name, 1),
		redisClient:  connection.redisClient,
	}
}

// closes a stale connection. not to be called on an active connection
func (connection *redisConnection) closeStaleConnection() error {
	if _, err := connection.redisClient.SRem(connectionsKey, connection.Name); err != nil {
		return err
	}
	if _, err := connection.redisClient.Del(connection.queuesKey); err != nil {
		return err
	}
	// debug(fmt.Sprintf("connection closed all queues %s %d", connection, connection.queuesKey)) // COMMENTOUT
	return nil
}

// openQueue opens a queue without adding it to the set of queues
func (connection *redisConnection) openQueue(name string) Queue {
	return newQueue(name, connection.Name, connection.queuesKey, connection.redisClient)
}

// flushDb flushes the redis database to reset everything, used in tests
func (connection *redisConnection) flushDb() error {
	return connection.redisClient.FlushDb()
}
