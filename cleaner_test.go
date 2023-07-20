package rmq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleaner(t *testing.T) {
	redisOptions, closer := testRedis(t)
	defer closer()

	flushConn, err := OpenConnectionWithRedisOptions("cleaner-flush", redisOptions, nil)
	assert.NoError(t, err)
	assert.NoError(t, flushConn.stopHeartbeat())
	assert.NoError(t, flushConn.flushDb())

	conn, err := OpenConnectionWithRedisOptions("cleaner-conn1", redisOptions, nil)
	assert.NoError(t, err)
	queues, err := conn.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 0)
	queue, err := conn.OpenQueue("q1")
	assert.NoError(t, err)
	queues, err = conn.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 1)
	_, err = conn.OpenQueue("q2")
	assert.NoError(t, err)
	queues, err = conn.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 2)

	eventuallyReady(t, queue, 0)
	assert.NoError(t, queue.Publish("del1"))
	eventuallyReady(t, queue, 1)
	assert.NoError(t, queue.Publish("del2"))
	eventuallyReady(t, queue, 2)
	assert.NoError(t, queue.Publish("del3"))
	eventuallyReady(t, queue, 3)
	assert.NoError(t, queue.Publish("del4"))
	eventuallyReady(t, queue, 4)
	assert.NoError(t, queue.Publish("del5"))
	eventuallyReady(t, queue, 5)
	assert.NoError(t, queue.Publish("del6"))
	eventuallyReady(t, queue, 6)

	eventuallyUnacked(t, queue, 0)
	assert.NoError(t, queue.StartConsuming(2, time.Millisecond))
	eventuallyUnacked(t, queue, 2)
	eventuallyReady(t, queue, 4)

	consumer := NewTestConsumer("c-A")
	consumer.AutoFinish = false
	consumer.AutoAck = false

	_, err = queue.AddConsumer("consumer1", consumer)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
	eventuallyUnacked(t, queue, 2)
	eventuallyReady(t, queue, 4)

	require.NotNil(t, consumer.Last())
	assert.Equal(t, "del1", consumer.Last().Payload())
	assert.NoError(t, consumer.Last().Ack())
	eventuallyUnacked(t, queue, 2)
	eventuallyReady(t, queue, 3)

	consumer.Finish()
	time.Sleep(10 * time.Millisecond)
	eventuallyUnacked(t, queue, 2)
	eventuallyReady(t, queue, 3)
	assert.Equal(t, "del2", consumer.Last().Payload())

	queue.StopConsuming()
	assert.NoError(t, conn.stopHeartbeat())
	time.Sleep(time.Millisecond)

	conn, err = OpenConnectionWithRedisOptions("cleaner-conn1", redisOptions, nil)
	assert.NoError(t, err)
	queue, err = conn.OpenQueue("q1")
	assert.NoError(t, err)

	assert.NoError(t, queue.Publish("del7"))
	eventuallyReady(t, queue, 4)
	assert.NoError(t, queue.Publish("del8"))
	eventuallyReady(t, queue, 5)
	assert.NoError(t, queue.Publish("del9"))
	eventuallyReady(t, queue, 6)
	assert.NoError(t, queue.Publish("del10"))
	eventuallyReady(t, queue, 7)
	assert.NoError(t, queue.Publish("del11"))
	eventuallyReady(t, queue, 8)

	eventuallyUnacked(t, queue, 0)
	assert.NoError(t, queue.StartConsuming(2, time.Millisecond))
	eventuallyUnacked(t, queue, 2)
	eventuallyReady(t, queue, 6)

	consumer = NewTestConsumer("c-B")
	consumer.AutoFinish = false
	consumer.AutoAck = false

	_, err = queue.AddConsumer("consumer2", consumer)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
	eventuallyUnacked(t, queue, 2)
	eventuallyReady(t, queue, 6)
	assert.Equal(t, "del4", consumer.Last().Payload())

	consumer.Finish() // unacked
	time.Sleep(10 * time.Millisecond)
	eventuallyUnacked(t, queue, 2)
	eventuallyReady(t, queue, 6)

	assert.Equal(t, "del5", consumer.Last().Payload())
	assert.NoError(t, consumer.Last().Ack())
	time.Sleep(10 * time.Millisecond)
	eventuallyUnacked(t, queue, 2)
	eventuallyReady(t, queue, 5)

	queue.StopConsuming()
	assert.NoError(t, conn.stopHeartbeat())
	time.Sleep(time.Millisecond)

	cleanerConn, err := OpenConnectionWithRedisOptions("cleaner-conn", redisOptions, nil)
	assert.NoError(t, err)
	cleaner := NewCleaner(cleanerConn)
	returned, err := cleaner.Clean()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), returned)
	eventuallyReady(t, queue, 9) // 2 of 11 were acked above
	queues, err = conn.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 2)

	conn, err = OpenConnectionWithRedisOptions("cleaner-conn1", redisOptions, nil)
	assert.NoError(t, err)
	queue, err = conn.OpenQueue("q1")
	assert.NoError(t, err)
	assert.NoError(t, queue.StartConsuming(10, time.Millisecond))
	consumer = NewTestConsumer("c-C")

	_, err = queue.AddConsumer("consumer3", consumer)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
	assert.Eventually(t, func() bool {
		return len(consumer.Deliveries()) == 9
	}, 10*time.Second, 2*time.Millisecond)

	queue.StopConsuming()
	assert.NoError(t, conn.stopHeartbeat())
	time.Sleep(time.Millisecond)

	returned, err = cleaner.Clean()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), returned)
	assert.NoError(t, cleanerConn.stopHeartbeat())
}
