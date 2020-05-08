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

const (
	heartbeatDuration = time.Minute // TTL of heartbeat key
	heartbeatInterval = time.Second // how often we update the heartbeat key
)

// Connection is an interface that can be used to test publishing
type Connection interface {
	OpenQueue(name string) (Queue, error)
	CollectStats(queueList []string) (Stats, error)
	GetOpenQueues() ([]string, error)

	// internals
	// used in cleaner
	checkHeartbeat() error
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

// OpenConnection opens and returns a new connection
func OpenConnection(tag, network, address string, db int, errors chan<- error) (Connection, error) {
	redisClient := redis.NewClient(&redis.Options{
		Network: network,
		Addr:    address,
		DB:      db,
	})
	return OpenConnectionWithRedisClient(tag, redisClient, errors)
}

// OpenConnectionWithRedisClient opens and returns a new connection
func OpenConnectionWithRedisClient(tag string, redisClient *redis.Client, errors chan<- error) (*redisConnection, error) {
	return openConnectionWithRedisClient(tag, RedisWrapper{redisClient}, errors)
}

// OpenConnectionWithTestRedisClient opens and returns a new connection which
// uses a test redis client internally. This is useful in integration tests.
func OpenConnectionWithTestRedisClient(tag string, errors chan<- error) (*redisConnection, error) {
	return openConnectionWithRedisClient(tag, NewTestRedisClient(), errors)
}

func openConnectionWithRedisClient(tag string, redisClient RedisClient, errors chan<- error) (*redisConnection, error) {
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

	go connection.heartbeat(errors)
	// log.Printf("rmq connection connected to %s %s:%s %d", name, network, address, db)
	return connection, nil
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

// checkHeartbeat retuns true if the connection is currently active in terms of heartbeat
func (connection *redisConnection) checkHeartbeat() error {
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
	count, err := connection.redisClient.Del(connection.heartbeatKey)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrorNotFound
	}
	return nil
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
func (connection *redisConnection) heartbeat(errors chan<- error) {
	errorCount := 0 // number of consecutive errors
	for range time.NewTicker(heartbeatInterval).C {
		if connection.heartbeatStopped {
			return
		}

		err := connection.updateHeartbeat()
		if err == nil { // success
			errorCount = 0
			continue
		}
		// unexpected redis error

		errorCount++
		select { // try to add error to channel, but don't block
		case errors <- &HeartbeatError{RedisErr: err, Count: errorCount}:
		default:
		}

		// TODO!: stop all consuming at some point (after 40s?)
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
	count, err := connection.redisClient.SRem(connectionsKey, connection.Name)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrorNotFound
	}

	// NOTE: we're not checking count here because stale connection might not
	// have been consuming from any queue, in which case this key doesn't exist
	if _, err = connection.redisClient.Del(connection.queuesKey); err != nil {
		return err
	}

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
