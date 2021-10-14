package rmq

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
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
	Name          string
	namespace     string
	heartbeatKey  string // key to keep alive
	queuesKey     string // key to list of queues consumed by this connection
	keys          keys
	redisClient   RedisClient
	errChan       chan<- error
	heartbeatStop chan chan struct{}

	lock    sync.Mutex
	stopped bool
	// list of all queues that have been opened in this connection
	// this is used to handle heartbeat errors without relying on the redis connection
	openQueues []Queue
}

// OpenConnection opens and returns a new connection
func OpenConnection(tag string, network string, address string, db int, errChan chan<- error) (Connection, error) {
	redisClient := redis.NewClient(&redis.Options{Network: network, Addr: address, DB: db})
	return OpenConnectionWithRedisClient(tag, redisClient, errChan)
}

// Option function type configures connection.
type Option func(*redisConnection)

// WithNamespace option adds namespace to connection.
func WithNamespace(namespace string) Option {
	return func(rc *redisConnection) {
		rc.namespace = namespace
	}
}

// WithTag option adds tag to connection.
func WithTag(tag string) Option {
	return func(rc *redisConnection) {
		rc.Name = fmt.Sprintf("%s-%s", tag, RandomString(6))
	}
}

// WithRedisClient option adds RedisClient to connection.
func WithRedisClient(redisClient RedisClient) Option {
	return func(rc *redisConnection) {
		rc.redisClient = redisClient
	}
}

// WithErrChan option adds err channel to connection.
func WithErrChan(errChan chan<- error) Option {
	return func(rc *redisConnection) {
		rc.errChan = errChan
	}
}

// OpenConnectionWithOptions configures connection.
func OpenConnectionWithOptions(opts ...Option) (Connection, error) {
	conn := &redisConnection{
		Name:          RandomString(6),
		heartbeatStop: make(chan chan struct{}, 1),
	}

	for _, opt := range opts {
		opt(conn)
	}

	connKeys := keys{namespace: conn.namespace}

	conn.keys = connKeys
	conn.heartbeatKey = connKeys.connectionHeartbeat(conn.Name)
	conn.queuesKey = connKeys.connectionQueues(conn.Name)

	if err := conn.updateHeartbeat(); err != nil { // checks the connection
		return nil, err
	}

	// add to connection set after setting heartbeat to avoid race with cleaner
	if _, err := conn.redisClient.SAdd(connKeys.connectionsKey(), conn.Name); err != nil {
		return nil, err
	}

	go conn.heartbeat(conn.errChan)

	return conn, nil
}

// OpenConnectionWithRedisClient opens and returns a new connection
func OpenConnectionWithRedisClient(tag string, redisClient redis.Cmdable, errChan chan<- error) (Connection, error) {
	return OpenConnectionWithRmqRedisClient(tag, RedisWrapper{redisClient}, errChan)
}

// OpenConnectionWithTestRedisClient opens and returns a new connection which
// uses a test redis client internally. This is useful in integration tests.
func OpenConnectionWithTestRedisClient(tag string, errChan chan<- error) (Connection, error) {
	return OpenConnectionWithRmqRedisClient(tag, NewTestRedisClient(), errChan)
}

// If you would like to use a redis client other than the ones supported in the constructors above, you can implement
// the RedisClient interface yourself
func OpenConnectionWithRmqRedisClient(tag string, redisClient RedisClient, errChan chan<- error) (Connection, error) {
	name := fmt.Sprintf("%s-%s", tag, RandomString(6))

	connKeys := keys{}

	connection := &redisConnection{
		Name:          name,
		keys:          connKeys,
		heartbeatKey:  connKeys.connectionHeartbeat(name),
		queuesKey:     connKeys.connectionQueues(name),
		redisClient:   redisClient,
		errChan:       errChan,
		heartbeatStop: make(chan chan struct{}, 1),
	}

	if err := connection.updateHeartbeat(); err != nil { // checks the connection
		return nil, err
	}

	// add to connection set after setting heartbeat to avoid race with cleaner
	if _, err := redisClient.SAdd(connKeys.connectionsKey(), name); err != nil {
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
		case c := <-connection.heartbeatStop:
			close(c)
			return
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
			connection.StopAllConsuming()
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

	if _, err := connection.redisClient.SAdd(connection.keys.queuesKey(), name); err != nil {
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
	return connection.redisClient.SMembers(connection.keys.queuesKey())
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

	// If we are already stopped or there are no open queues, then there is nothing to do
	if connection.stopped || len(connection.openQueues) == 0 {
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
		close(finishedChan)
		// log.Printf("rmq connection stopped consuming %s", queue)
	}()

	return finishedChan
}

// checkHeartbeat retuns true if the connection is currently active in terms of heartbeat
func (connection *redisConnection) checkHeartbeat() error {
	heartbeatKey := connection.keys.connectionHeartbeat(connection.Name)
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
	return connection.redisClient.SMembers(connection.keys.connectionsKey())
}

// hijackConnection reopens an existing connection for inspection purposes without starting a heartbeat
func (connection *redisConnection) hijackConnection(name string) Connection {
	return &redisConnection{
		Name:         name,
		heartbeatKey: connection.keys.connectionHeartbeat(name),
		queuesKey:    connection.keys.connectionQueues(name),
		redisClient:  connection.redisClient,
	}
}

// closes a stale connection. not to be called on an active connection
func (connection *redisConnection) closeStaleConnection() error {
	count, err := connection.redisClient.SRem(connection.keys.connectionsKey(), connection.Name)
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
		connection.namespace,
		connection.Name,
		connection.queuesKey,
		connection.redisClient,
		connection.errChan,
	)
}

// stopHeartbeat stops the heartbeat of the connection
// it does not remove it from the list of connections so it can later be found by the cleaner
func (connection *redisConnection) stopHeartbeat() error {
	if connection.heartbeatStop == nil {
		return ErrorNotFound
	}

	heartbeatStopped := make(chan struct{})
	connection.heartbeatStop <- heartbeatStopped
	<-heartbeatStopped
	connection.heartbeatStop = nil // avoid stopping twice

	count, err := connection.redisClient.Del(connection.heartbeatKey)
	if err != nil {
		return err
	}
	if count == 0 {
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
	_, err := connection.redisClient.Del(connection.keys.queuesKey())
	return err
}
