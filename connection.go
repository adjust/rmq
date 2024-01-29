package rmq

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// NOTE: Be careful when changing any of these values.
	// Currently we update the heartbeat every second with a TTL of a minute.
	// This means that if we fail to update the heartbeat 60 times in a row
	// the connection might get cleaned up by a cleaner. So we want to set the
	// error limit to a value lower like this (like 45) to make sure we stop
	// all consuming before that happens.
	heartbeatDuration   = time.Minute // TTL of heartbeat key
	heartbeatInterval   = time.Second // how often we update the heartbeat key
	HeartbeatErrorLimit = 45          // stop consuming after this many heartbeat errors
)

// Connection is an interface that can be used to test publishing
type Connection interface {
	OpenQueue(name string) (Queue, error)
	CollectStats(queueList []string) (Stats, error)
	GetOpenQueues() ([]string, error)
	StopAllConsuming() <-chan struct{}

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
	Name         string
	heartbeatKey string // key to keep alive
	queuesKey    string // key to list of queues consumed by this connection

	consumersTemplate string
	unackedTemplate   string
	readyTemplate     string
	rejectedTemplate  string

	redisClient   RedisClient
	errChan       chan<- error
	heartbeatStop chan chan struct{} // used to stop heartbeat() in stopHeartbeat(), nil once stopped

	lock    sync.Mutex
	stopped bool
	// list of all queues that have been opened in this connection
	// this is used to handle heartbeat errors without relying on the redis connection
	openQueues []Queue
}

// OpenConnection opens and returns a new connection
func OpenConnection(tag string, network string, address string, db int, errChan chan<- error) (Connection, error) {
	return OpenConnectionWithRedisOptions(tag, &redis.Options{Network: network, Addr: address, DB: db}, errChan)
}

// OpenConnectionWithRedisOptions allows you to pass more flexible options
func OpenConnectionWithRedisOptions(tag string, redisOption *redis.Options, errChan chan<- error) (Connection, error) {
	return OpenConnectionWithRedisClient(tag, redis.NewClient(redisOption), errChan)
}

// OpenConnectionWithRedisClient opens and returns a new connection
// This can be used to passa redis.ClusterClient.
func OpenConnectionWithRedisClient(tag string, redisClient redis.Cmdable, errChan chan<- error) (Connection, error) {
	return OpenConnectionWithRmqRedisClient(tag, RedisWrapper{redisClient}, errChan)
}

// OpenConnectionWithTestRedisClient opens and returns a new connection which
// uses a test redis client internally. This is useful in integration tests.
func OpenConnectionWithTestRedisClient(tag string, errChan chan<- error) (Connection, error) {
	return OpenConnectionWithRmqRedisClient(tag, NewTestRedisClient(), errChan)
}

// OpenConnectionWithRmqRedisClient: If you would like to use a redis client other than the ones
// supported in the constructors above, you can implement the RedisClient interface yourself
func OpenConnectionWithRmqRedisClient(tag string, redisClient RedisClient, errChan chan<- error) (Connection, error) {
	return openConnection(tag, redisClient, false, errChan)
}

// OpenClusterConnection: Same as OpenConnectionWithRedisClient, but using Redis hash tags {} instead of [].
func OpenClusterConnection(tag string, redisClient redis.Cmdable, errChan chan<- error) (Connection, error) {
	return openConnection(tag, RedisWrapper{redisClient}, true, errChan)
}

func openConnection(tag string, redisClient RedisClient, useRedisHashTags bool, errChan chan<- error) (Connection, error) {
	name := fmt.Sprintf("%s-%s", tag, RandomString(6))

	connection := &redisConnection{
		Name:              name,
		heartbeatKey:      strings.Replace(connectionHeartbeatTemplate, phConnection, name, 1),
		queuesKey:         strings.Replace(connectionQueuesTemplate, phConnection, name, 1),
		consumersTemplate: getTemplate(connectionQueueConsumersBaseTemplate, useRedisHashTags),
		unackedTemplate:   getTemplate(connectionQueueUnackedBaseTemplate, useRedisHashTags),
		readyTemplate:     getTemplate(queueReadyBaseTemplate, useRedisHashTags),
		rejectedTemplate:  getTemplate(queueRejectedBaseTemplate, useRedisHashTags),
		redisClient:       redisClient,
		errChan:           errChan,
		heartbeatStop:     make(chan chan struct{}, 1), // mark heartbeat as active, can be stopped
	}

	if err := connection.updateHeartbeat(); err != nil { // checks the connection
		return nil, err
	}

	// add to connection set after setting heartbeat to avoid race with cleaner
	if _, err := redisClient.SAdd(connectionsKey, name); err != nil {
		return nil, err
	}

	go connection.heartbeat(errChan)
	// log.Printf("rmq connection connected to %s %s:%s %d", name, network, address, db)
	return connection, nil
}

func (connection *redisConnection) updateHeartbeat() error {
	return connection.redisClient.Set(connection.heartbeatKey, "1", heartbeatDuration)
}

// heartbeat keeps the heartbeat key alive
func (connection *redisConnection) heartbeat(errChan chan<- error) {
	errorCount := 0 // number of consecutive errors

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// continue below
		case c := <-connection.heartbeatStop: // stopHeartbeat() has been called
			close(c) // confirm to stopHeartbeat() that the heartbeat is stopped
			return   // stop updating the heartbeat
		}

		err := connection.updateHeartbeat()
		if err == nil { // success
			errorCount = 0
			continue
		}
		// unexpected redis error

		errorCount++

		if errorCount >= HeartbeatErrorLimit {
			// reached error limit

			// To avoid using this connection while we're not able to maintain its heartbeat we stop all
			// consumers. This in turn will call stopHeartbeat() and the responsibility of heartbeat() to
			// confirm that the heartbeat is stopped, so we do that here too.
			connection.StopAllConsuming()
			close(<-connection.heartbeatStop) // wait for stopHeartbeat() and confirm heartbeat is stopped

			// Clients reading from errChan need to see this error
			// This allows them to shut themselves down
			// Therefore we block adding it to errChan to ensure delivery
			errChan <- &HeartbeatError{RedisErr: err, Count: errorCount}
			return
		} else {
			select { // try to add error to channel, but don't block
			case errChan <- &HeartbeatError{RedisErr: err, Count: errorCount}:
			default:
			}
		}
		// keep trying until we hit the limit
	}
}

func (connection *redisConnection) String() string {
	return connection.Name
}

// OpenQueue opens and returns the queue with a given name
func (connection *redisConnection) OpenQueue(name string) (Queue, error) {
	connection.lock.Lock()
	defer connection.lock.Unlock()

	if connection.stopped {
		return nil, ErrorConsumingStopped
	}

	if _, err := connection.redisClient.SAdd(queuesKey, name); err != nil {
		return nil, err
	}

	queue := connection.openQueue(name)
	connection.openQueues = append(connection.openQueues, queue)

	return queue, nil
}

// CollectStats collects and returns stats
func (connection *redisConnection) CollectStats(queueList []string) (Stats, error) {
	return CollectStats(queueList, connection)
}

// GetOpenQueues returns a list of all open queues
func (connection *redisConnection) GetOpenQueues() ([]string, error) {
	return connection.redisClient.SMembers(queuesKey)
}

// StopAllConsuming stops consuming on all queues opened in this connection.
// It returns a channel which can be used to wait for all active consumers to
// finish their current Consume() call. This is useful to implement graceful
// shutdown.
func (connection *redisConnection) StopAllConsuming() <-chan struct{} {
	connection.lock.Lock()
	defer func() {
		// regardless of how we exit this method, the connection is always stopped when we return
		connection.stopped = true
		connection.lock.Unlock()
	}()

	finishedChan := make(chan struct{})

	// If we are already stopped then there is nothing to do
	if connection.stopped {
		close(finishedChan)
		return finishedChan
	}

	// If there are no open queues we still want to stop the heartbeat
	if len(connection.openQueues) == 0 {
		connection.stopHeartbeat()
		close(finishedChan)
		return finishedChan
	}

	chans := make([]<-chan struct{}, 0, len(connection.openQueues))
	for _, queue := range connection.openQueues {
		chans = append(chans, queue.StopConsuming())
	}

	go func() {
		// wait for all channels to be closed
		for _, c := range chans {
			<-c
		}

		// All consuming has been stopped. Now we can stop the heartbeat to avoid a goroutine leak.
		connection.stopHeartbeat()

		close(finishedChan) // signal all done
	}()

	return finishedChan
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

// getConnections returns a list of all open connections
func (connection *redisConnection) getConnections() ([]string, error) {
	return connection.redisClient.SMembers(connectionsKey)
}

// hijackConnection reopens an existing connection for inspection purposes without starting a heartbeat
func (connection *redisConnection) hijackConnection(name string) Connection {
	return &redisConnection{
		Name:              name,
		heartbeatKey:      strings.Replace(connectionHeartbeatTemplate, phConnection, name, 1),
		queuesKey:         strings.Replace(connectionQueuesTemplate, phConnection, name, 1),
		consumersTemplate: connection.consumersTemplate,
		unackedTemplate:   connection.unackedTemplate,
		readyTemplate:     connection.readyTemplate,
		rejectedTemplate:  connection.rejectedTemplate,
		redisClient:       connection.redisClient,
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

// getConsumingQueues returns a list of all queues consumed by this connection
func (connection *redisConnection) getConsumingQueues() ([]string, error) {
	return connection.redisClient.SMembers(connection.queuesKey)
}

// openQueue opens a queue without adding it to the set of queues
func (connection *redisConnection) openQueue(name string) Queue {
	return newQueue(
		name,
		connection.Name,
		connection.queuesKey,
		connection.consumersTemplate,
		connection.unackedTemplate,
		connection.readyTemplate,
		connection.rejectedTemplate,
		connection.redisClient,
		connection.errChan,
	)
}

// stopHeartbeat stops the heartbeat of the connection.
// It does not remove it from the list of connections so it can later be found by the cleaner.
// Returns ErrorNotFound if the heartbeat was already stopped.
// Note that this function itself is not threadsafe, it's important to not call it multiple times
// at the same time. Currently it's only called in StopAllConsuming() where it's linearized by
// connection.lock.
func (connection *redisConnection) stopHeartbeat() error {
	if connection.heartbeatStop == nil { // already stopped
		return ErrorNotFound
	}

	heartbeatStopped := make(chan struct{})
	connection.heartbeatStop <- heartbeatStopped
	<-heartbeatStopped             // wait for heartbeat() to confirm it's stopped
	connection.heartbeatStop = nil // mark heartbeat as stopped

	// Delete heartbeat key to immediately make the connection appear inactive to the cleaner,
	// instead of waiting for the heartbeat key to run into its TTL.
	count, err := connection.redisClient.Del(connection.heartbeatKey)
	if err != nil { // redis error
		return err
	}
	if count == 0 { // heartbeat key didn't exist
		return ErrorNotFound
	}
	return nil
}

// flushDb flushes the redis database to reset everything, used in tests
func (connection *redisConnection) flushDb() error {
	return connection.redisClient.FlushDb()
}

// unlistAllQueues closes all queues by removing them from the global list
func (connection *redisConnection) unlistAllQueues() error {
	_, err := connection.redisClient.Del(queuesKey)
	return err
}
