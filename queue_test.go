package rmq

import (
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnections(t *testing.T) {
	flushConn, err := OpenConnection("conns-flush", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	assert.NoError(t, flushConn.stopHeartbeat())
	assert.NoError(t, flushConn.flushDb())

	connection, err := OpenConnection("conns-conn", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	require.NotNil(t, connection)
	_, err = NewCleaner(connection).Clean()
	require.NoError(t, err)

	connections, err := connection.getConnections()
	assert.NoError(t, err)
	assert.Len(t, connections, 1) // cleaner connection remains

	conn1, err := OpenConnection("conns-conn1", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	connections, err = connection.getConnections()
	assert.NoError(t, err)
	assert.Len(t, connections, 2)
	assert.Equal(t, ErrorNotFound, connection.hijackConnection("nope").checkHeartbeat())
	assert.NoError(t, conn1.checkHeartbeat())
	conn2, err := OpenConnection("conns-conn2", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	connections, err = connection.getConnections()
	assert.NoError(t, err)
	assert.Len(t, connections, 3)
	assert.NoError(t, conn1.checkHeartbeat())
	assert.NoError(t, conn2.checkHeartbeat())

	assert.Equal(t, ErrorNotFound, connection.hijackConnection("nope").stopHeartbeat())
	assert.NoError(t, conn1.stopHeartbeat())
	assert.Equal(t, ErrorNotFound, conn1.checkHeartbeat())
	assert.NoError(t, conn2.checkHeartbeat())
	connections, err = connection.getConnections()
	assert.NoError(t, err)
	assert.Len(t, connections, 3)

	assert.NoError(t, conn2.stopHeartbeat())
	assert.Equal(t, ErrorNotFound, conn1.checkHeartbeat())
	assert.Equal(t, ErrorNotFound, conn2.checkHeartbeat())
	connections, err = connection.getConnections()
	assert.NoError(t, err)
	assert.Len(t, connections, 3)

	assert.NoError(t, connection.stopHeartbeat())
}

func TestConnectionQueues(t *testing.T) {
	connection, err := OpenConnection("conn-q-conn", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	require.NotNil(t, connection)

	assert.NoError(t, connection.unlistAllQueues())
	queues, err := connection.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 0)

	queue1, err := connection.OpenQueue("conn-q-q1")
	assert.NoError(t, err)
	require.NotNil(t, queue1)
	queues, err = connection.GetOpenQueues()
	assert.NoError(t, err)
	assert.Equal(t, []string{"conn-q-q1"}, queues)
	queues, err = connection.getConsumingQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 0)
	assert.NoError(t, queue1.StartConsuming(1, time.Millisecond))
	queues, err = connection.getConsumingQueues()
	assert.NoError(t, err)
	assert.Equal(t, []string{"conn-q-q1"}, queues)

	queue2, err := connection.OpenQueue("conn-q-q2")
	assert.NoError(t, err)
	require.NotNil(t, queue2)
	queues, err = connection.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 2)
	queues, err = connection.getConsumingQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 1)
	assert.NoError(t, queue2.StartConsuming(1, time.Millisecond))
	queues, err = connection.getConsumingQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 2)

	queue2.StopConsuming()
	assert.NoError(t, queue2.closeInStaleConnection())
	queues, err = connection.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 2)
	queues, err = connection.getConsumingQueues()
	assert.NoError(t, err)
	assert.Equal(t, []string{"conn-q-q1"}, queues)

	queue1.StopConsuming()
	assert.NoError(t, queue1.closeInStaleConnection())
	queues, err = connection.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 2)
	queues, err = connection.getConsumingQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 0)

	readyCount, rejectedCount, err := queue1.Destroy()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), readyCount)
	assert.Equal(t, int64(0), rejectedCount)
	queues, err = connection.GetOpenQueues()
	assert.NoError(t, err)
	assert.Equal(t, []string{"conn-q-q2"}, queues)
	queues, err = connection.getConsumingQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 0)

	assert.NoError(t, connection.stopHeartbeat())
}

func TestQueueCommon(t *testing.T) {
	connection, err := OpenConnection("queue-conn", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	require.NotNil(t, connection)

	queue, err := connection.OpenQueue("queue-q")
	assert.NoError(t, err)
	require.NotNil(t, queue)
	_, err = queue.PurgeReady()
	assert.NoError(t, err)
	count, err := queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	assert.NoError(t, queue.Publish("queue-d1"))
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.NoError(t, queue.Publish("queue-d2"))
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
	count, err = queue.PurgeReady()
	assert.Equal(t, int64(2), count)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue.PurgeReady()
	assert.Equal(t, int64(0), count)

	queues, err := connection.getConsumingQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 0)
	assert.NoError(t, queue.StartConsuming(10, time.Millisecond))
	assert.Equal(t, ErrorAlreadyConsuming, queue.StartConsuming(10, time.Millisecond))
	cons1name, err := queue.AddConsumer("queue-cons1", NewTestConsumer("queue-A"))
	assert.NoError(t, err)
	time.Sleep(time.Millisecond)
	queues, err = connection.getConsumingQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, 1)
	consumers, err := queue.getConsumers()
	assert.NoError(t, err)
	assert.Equal(t, []string{cons1name}, consumers)
	_, err = queue.AddConsumer("queue-cons2", NewTestConsumer("queue-B"))
	assert.NoError(t, err)
	consumers, err = queue.getConsumers()
	assert.NoError(t, err)
	assert.Len(t, consumers, 2)

	queue.StopConsuming()
	assert.NoError(t, connection.stopHeartbeat())
}

func TestConsumerCommon(t *testing.T) {
	connection, err := OpenConnection("cons-conn", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	require.NotNil(t, connection)

	queue1, err := connection.OpenQueue("cons-q")
	assert.NoError(t, err)
	require.NotNil(t, queue1)
	_, err = queue1.PurgeReady()
	assert.NoError(t, err)

	consumer := NewTestConsumer("cons-A")
	consumer.AutoAck = false
	assert.NoError(t, queue1.StartConsuming(10, time.Millisecond))
	_, err = queue1.AddConsumer("cons-cons", consumer)
	assert.NoError(t, err)
	assert.Nil(t, consumer.LastDelivery)

	assert.NoError(t, queue1.Publish("cons-d1"))
	time.Sleep(2 * time.Millisecond)
	require.NotNil(t, consumer.LastDelivery)
	assert.Equal(t, "cons-d1", consumer.LastDelivery.Payload())
	count, err := queue1.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue1.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	assert.NoError(t, queue1.Publish("cons-d2"))
	time.Sleep(2 * time.Millisecond)
	assert.Equal(t, "cons-d2", consumer.LastDelivery.Payload())
	count, err = queue1.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue1.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	assert.NoError(t, consumer.LastDeliveries[0].Ack())
	count, err = queue1.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue1.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	assert.NoError(t, consumer.LastDeliveries[1].Ack())
	count, err = queue1.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue1.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)

	assert.Equal(t, ErrorNotFound, consumer.LastDeliveries[0].Ack())

	assert.NoError(t, queue1.Publish("cons-d3"))
	time.Sleep(2 * time.Millisecond)
	count, err = queue1.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue1.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
	count, err = queue1.rejectedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	assert.Equal(t, "cons-d3", consumer.LastDelivery.Payload())
	assert.NoError(t, consumer.LastDelivery.Reject())
	count, err = queue1.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue1.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue1.rejectedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	assert.NoError(t, queue1.Publish("cons-d4"))
	time.Sleep(2 * time.Millisecond)
	count, err = queue1.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue1.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
	count, err = queue1.rejectedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
	assert.Equal(t, "cons-d4", consumer.LastDelivery.Payload())
	assert.NoError(t, consumer.LastDelivery.Reject())
	count, err = queue1.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue1.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue1.rejectedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
	count, err = queue1.PurgeRejected()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)
	count, err = queue1.rejectedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue1.PurgeRejected()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)

	queue2, err := connection.OpenQueue("cons-func-q")
	assert.NoError(t, err)
	assert.NoError(t, queue2.StartConsuming(10, time.Millisecond))

	payloadChan := make(chan string, 1)
	payload := "cons-func-payload"

	_, err = queue2.AddConsumerFunc("cons-func", func(delivery Delivery) {
		err = delivery.Ack()
		assert.NoError(t, err)
		payloadChan <- delivery.Payload()
	})
	assert.NoError(t, err)

	assert.NoError(t, queue2.Publish(payload))
	time.Sleep(2 * time.Millisecond)
	assert.Equal(t, payload, <-payloadChan)
	count, err = queue2.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue2.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)

	queue1.StopConsuming()
	queue2.StopConsuming()
	assert.NoError(t, connection.stopHeartbeat())
}

func TestMulti(t *testing.T) {
	connection, err := OpenConnection("multi-conn", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	queue, err := connection.OpenQueue("multi-q")
	assert.NoError(t, err)
	_, err = queue.PurgeReady()
	assert.NoError(t, err)

	for i := 0; i < 20; i++ {
		err := queue.Publish(fmt.Sprintf("multi-d%d", i))
		assert.NoError(t, err)
	}
	count, err := queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(20), count)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)

	assert.NoError(t, queue.StartConsuming(10, time.Millisecond))
	time.Sleep(2 * time.Millisecond)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), count)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), count)

	consumer := NewTestConsumer("multi-cons")
	consumer.AutoAck = false
	consumer.AutoFinish = false

	_, err = queue.AddConsumer("multi-cons", consumer)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), count)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), count)

	assert.NoError(t, consumer.LastDelivery.Ack())
	time.Sleep(10 * time.Millisecond)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(9), count)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), count)

	consumer.Finish()
	time.Sleep(10 * time.Millisecond)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(9), count)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), count)

	assert.NoError(t, consumer.LastDelivery.Ack())
	time.Sleep(10 * time.Millisecond)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(8), count)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), count)

	consumer.Finish()
	time.Sleep(10 * time.Millisecond)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(8), count)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(10), count)

	queue.StopConsuming()
	assert.NoError(t, connection.stopHeartbeat())
}

func TestBatch(t *testing.T) {
	connection, err := OpenConnection("batch-conn", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	queue, err := connection.OpenQueue("batch-q")
	assert.NoError(t, err)
	_, err = queue.PurgeRejected()
	assert.NoError(t, err)
	_, err = queue.PurgeReady()
	assert.NoError(t, err)

	for i := 0; i < 5; i++ {
		err := queue.Publish(fmt.Sprintf("batch-d%d", i))
		assert.NoError(t, err)
	}

	assert.NoError(t, queue.StartConsuming(10, time.Millisecond))
	time.Sleep(10 * time.Millisecond)
	count, err := queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(5), count)

	consumer := NewTestBatchConsumer()
	_, err = queue.AddBatchConsumer("batch-cons", 2, 50*time.Millisecond, consumer)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
	require.Len(t, consumer.LastBatch, 2)
	assert.Equal(t, "batch-d0", consumer.LastBatch[0].Payload())
	assert.Equal(t, "batch-d1", consumer.LastBatch[1].Payload())
	assert.NoError(t, consumer.LastBatch[0].Reject())
	assert.NoError(t, consumer.LastBatch[1].Ack())
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)
	count, err = queue.rejectedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	consumer.Finish()
	time.Sleep(10 * time.Millisecond)
	require.Len(t, consumer.LastBatch, 2)
	assert.Equal(t, "batch-d2", consumer.LastBatch[0].Payload())
	assert.Equal(t, "batch-d3", consumer.LastBatch[1].Payload())
	assert.NoError(t, consumer.LastBatch[0].Reject())
	assert.NoError(t, consumer.LastBatch[1].Ack())
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
	count, err = queue.rejectedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	consumer.Finish()
	time.Sleep(10 * time.Millisecond)
	assert.Len(t, consumer.LastBatch, 0)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
	count, err = queue.rejectedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count)

	time.Sleep(60 * time.Millisecond)
	require.Len(t, consumer.LastBatch, 1)
	assert.Equal(t, "batch-d4", consumer.LastBatch[0].Payload())
	assert.NoError(t, consumer.LastBatch[0].Reject())
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue.rejectedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(3), count)
}

func TestReturnRejected(t *testing.T) {
	connection, err := OpenConnection("return-conn", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	queue, err := connection.OpenQueue("return-q")
	assert.NoError(t, err)
	_, err = queue.PurgeReady()
	assert.NoError(t, err)

	for i := 0; i < 6; i++ {
		err := queue.Publish(fmt.Sprintf("return-d%d", i))
		assert.NoError(t, err)
	}

	count, err := queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(6), count)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue.rejectedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)

	assert.NoError(t, queue.StartConsuming(10, time.Millisecond))
	time.Sleep(time.Millisecond)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(6), count)
	count, err = queue.rejectedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)

	consumer := NewTestConsumer("return-cons")
	consumer.AutoAck = false
	_, err = queue.AddConsumer("cons", consumer)
	assert.NoError(t, err)
	time.Sleep(time.Millisecond)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(6), count)
	count, err = queue.rejectedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)

	assert.Len(t, consumer.LastDeliveries, 6)
	assert.NoError(t, consumer.LastDeliveries[0].Reject())
	assert.NoError(t, consumer.LastDeliveries[1].Ack())
	assert.NoError(t, consumer.LastDeliveries[2].Reject())
	assert.NoError(t, consumer.LastDeliveries[3].Reject())
	// delivery 4 still open
	assert.NoError(t, consumer.LastDeliveries[5].Reject())

	time.Sleep(time.Millisecond)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count) // delivery 4
	count, err = queue.rejectedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count) // delivery 0, 2, 3, 5

	queue.StopConsuming()

	n, err := queue.ReturnRejected(2)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), n)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count) // delivery 0, 2
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count) // delivery 4
	count, err = queue.rejectedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(2), count) // delivery 3, 5

	n, err = queue.ReturnRejected(math.MaxInt64)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), n)
	count, err = queue.readyCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(4), count) // delivery 0, 2, 3, 5
	count, err = queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count) // delivery 4
	count, err = queue.rejectedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestPushQueue(t *testing.T) {
	connection, err := OpenConnection("push", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	queue1, err := connection.OpenQueue("queue1")
	assert.NoError(t, err)
	queue2, err := connection.OpenQueue("queue2")
	assert.NoError(t, err)
	queue1.SetPushQueue(queue2)
	assert.Equal(t, queue2.(*redisQueue).readyKey, queue1.(*redisQueue).pushKey)

	consumer1 := NewTestConsumer("push-cons")
	consumer1.AutoAck = false
	consumer1.AutoFinish = false
	assert.NoError(t, queue1.StartConsuming(10, time.Millisecond))
	_, err = queue1.AddConsumer("push-cons", consumer1)
	assert.NoError(t, err)

	consumer2 := NewTestConsumer("push-cons")
	consumer2.AutoAck = false
	consumer2.AutoFinish = false
	assert.NoError(t, queue2.StartConsuming(10, time.Millisecond))
	_, err = queue2.AddConsumer("push-cons", consumer2)
	assert.NoError(t, err)

	assert.NoError(t, queue1.Publish("d1"))
	time.Sleep(2 * time.Millisecond)
	count, err := queue1.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
	require.Len(t, consumer1.LastDeliveries, 1)

	assert.NoError(t, consumer1.LastDelivery.Push())
	time.Sleep(2 * time.Millisecond)
	count, err = queue1.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), count)
	count, err = queue2.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	require.Len(t, consumer2.LastDeliveries, 1)
	assert.NoError(t, consumer2.LastDelivery.Push())
	time.Sleep(2 * time.Millisecond)
	count, err = queue2.rejectedCount()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)
}

func TestConsuming(t *testing.T) {
	connection, err := OpenConnection("consume", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	queue, err := connection.OpenQueue("consume-q")
	assert.NoError(t, err)

	finishedChan := queue.StopConsuming()
	assert.NotNil(t, finishedChan)
	select {
	case <-finishedChan:
	default:
		t.FailNow() // should return closed finishedChan
	}

	assert.NoError(t, queue.StartConsuming(10, time.Millisecond))
	assert.NotNil(t, queue.StopConsuming())
	// already stopped
	assert.NotNil(t, queue.StopConsuming())
	select {
	case <-finishedChan:
	default:
		t.FailNow() // should return closed finishedChan
	}
}

func TestStopConsuming_Consumer(t *testing.T) {
	connection, err := OpenConnection("consume", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	queue, err := connection.OpenQueue("consume-q")
	assert.NoError(t, err)
	_, err = queue.PurgeReady()
	assert.NoError(t, err)

	deliveryCount := int64(30)

	for i := int64(0); i < deliveryCount; i++ {
		err := queue.Publish("d" + strconv.FormatInt(i, 10))
		assert.NoError(t, err)
	}

	assert.NoError(t, queue.StartConsuming(20, time.Millisecond))
	var consumers []*TestConsumer
	for i := 0; i < 10; i++ {
		consumer := NewTestConsumer("c" + strconv.Itoa(i))
		consumers = append(consumers, consumer)
		_, err = queue.AddConsumer("consume", consumer)
		assert.NoError(t, err)
	}

	finishedChan := queue.StopConsuming()
	require.NotNil(t, finishedChan)

	<-finishedChan

	var consumedCount int64
	for i := 0; i < 10; i++ {
		consumedCount += int64(len(consumers[i].LastDeliveries))
	}

	// make sure all deliveries are either ready, unacked or consumed (acked)
	readyCount, err := queue.readyCount()
	assert.NoError(t, err)
	unackedCount, err := queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, readyCount+unackedCount+consumedCount, deliveryCount, "counts %d+%d+%d = %d", consumedCount, readyCount, unackedCount, deliveryCount)

	assert.NoError(t, connection.stopHeartbeat())
}

func TestStopConsuming_BatchConsumer(t *testing.T) {
	connection, err := OpenConnection("batchConsume", "tcp", "localhost:6379", 1, nil)
	assert.NoError(t, err)
	queue, err := connection.OpenQueue("batchConsume-q")
	assert.NoError(t, err)
	_, err = queue.PurgeReady()
	assert.NoError(t, err)

	deliveryCount := int64(50)

	for i := int64(0); i < deliveryCount; i++ {
		err := queue.Publish("d" + strconv.FormatInt(i, 10))
		assert.NoError(t, err)
	}

	assert.NoError(t, queue.StartConsuming(20, time.Millisecond))

	var consumers []*TestBatchConsumer
	for i := 0; i < 10; i++ {
		consumer := NewTestBatchConsumer()
		consumer.AutoFinish = true
		consumers = append(consumers, consumer)
		_, err = queue.AddBatchConsumer("consume", 5, time.Second, consumer)
		assert.NoError(t, err)
	}

	time.Sleep(2 * time.Millisecond)
	finishedChan := queue.StopConsuming()
	require.NotNil(t, finishedChan)

	<-finishedChan

	var consumedCount int64
	for i := 0; i < 10; i++ {
		consumedCount += consumers[i].ConsumedCount
	}

	// make sure all deliveries are either ready, unacked or consumed (acked)
	readyCount, err := queue.readyCount()
	assert.NoError(t, err)
	unackedCount, err := queue.unackedCount()
	assert.NoError(t, err)
	assert.Equal(t, readyCount+unackedCount+consumedCount, deliveryCount, "counts %d+%d+%d = %d", consumedCount, readyCount, unackedCount, deliveryCount)

	assert.NoError(t, connection.stopHeartbeat())
}

func BenchmarkQueue(b *testing.B) {
	// open queue
	connection, err := OpenConnection("bench-conn", "tcp", "localhost:6379", 1, nil)
	assert.NoError(b, err)
	queueName := fmt.Sprintf("bench-q%d", b.N)
	queue, err := connection.OpenQueue(queueName)
	assert.NoError(b, err)
	assert.NoError(b, queue.StartConsuming(10, time.Millisecond))

	// add some consumers
	numConsumers := 10
	var consumers []*TestConsumer
	for i := 0; i < numConsumers; i++ {
		consumer := NewTestConsumer("bench-A")
		// consumer.SleepDuration = time.Microsecond
		consumers = append(consumers, consumer)
		_, err = queue.AddConsumer("bench-cons", consumer)
		assert.NoError(b, err)
	}

	// publish deliveries
	for i := 0; i < b.N; i++ {
		err := queue.Publish("bench-d")
		assert.NoError(b, err)
	}

	// wait until all are consumed
	for {
		ready, err := queue.readyCount()
		assert.NoError(b, err)
		unacked, err := queue.unackedCount()
		assert.NoError(b, err)
		fmt.Printf("%d unacked %d %d\n", b.N, ready, unacked)
		if ready == 0 && unacked == 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}

	time.Sleep(time.Millisecond)

	sum := 0
	for _, consumer := range consumers {
		sum += len(consumer.LastDeliveries)
	}
	fmt.Printf("consumed %d\n", sum)

	assert.NoError(b, connection.stopHeartbeat())
}
