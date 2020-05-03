package rmq

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	. "github.com/adjust/gocheck"
)

func TestQueueSuite(t *testing.T) {
	TestingSuiteT(&QueueSuite{}, t)
}

type QueueSuite struct{}

func (suite *QueueSuite) TestConnections(c *C) {
	flushConn, err := OpenConnection("conns-flush", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	flushConn.flushDb()
	flushConn.stopHeartbeat()

	connection, err := OpenConnection("conns-conn", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	c.Assert(connection, NotNil)
	c.Assert(NewCleaner(connection).Clean(), IsNil)

	connections, err := connection.getConnections()
	c.Check(err, IsNil)
	c.Check(connections, HasLen, 1, Commentf("cleaner %s", connection.Name)) // cleaner connection remains

	conn1, err := OpenConnection("conns-conn1", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	connections, err = connection.getConnections()
	c.Check(err, IsNil)
	c.Check(connections, HasLen, 2)
	err = connection.hijackConnection("nope").check()
	c.Check(err, Equals, ErrorNotFound)
	err = conn1.check()
	c.Check(err, IsNil)
	conn2, err := OpenConnection("conns-conn2", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	connections, err = connection.getConnections()
	c.Check(err, IsNil)
	c.Check(connections, HasLen, 3)
	err = conn1.check()
	c.Check(err, IsNil)
	err = conn2.check()
	c.Check(err, IsNil)

	connection.hijackConnection("nope").stopHeartbeat()
	conn1.stopHeartbeat()
	err = conn1.check()
	c.Check(err, Equals, ErrorNotFound)
	err = conn2.check()
	c.Check(err, IsNil)
	connections, err = connection.getConnections()
	c.Check(err, IsNil)
	c.Check(connections, HasLen, 3)

	conn2.stopHeartbeat()
	err = conn1.check()
	c.Check(err, Equals, ErrorNotFound)
	err = conn2.check()
	c.Check(err, Equals, ErrorNotFound)
	connections, err = connection.getConnections()
	c.Check(err, IsNil)
	c.Check(connections, HasLen, 3)

	connection.stopHeartbeat()
}

func (suite *QueueSuite) TestConnectionQueues(c *C) {
	connection, err := OpenConnection("conn-q-conn", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	c.Assert(connection, NotNil)

	connection.CloseAllQueues()
	queues, err := connection.GetOpenQueues()
	c.Check(err, IsNil)
	c.Check(queues, HasLen, 0)

	queue1, err := connection.OpenQueue("conn-q-q1")
	c.Check(err, IsNil)
	c.Assert(queue1, NotNil)
	queues, err = connection.GetOpenQueues()
	c.Check(err, IsNil)
	c.Check(queues, DeepEquals, []string{"conn-q-q1"})
	queues, err = connection.getConsumingQueues()
	c.Check(err, IsNil)
	c.Check(queues, HasLen, 0)
	queue1.StartConsuming(1, time.Millisecond)
	queues, err = connection.getConsumingQueues()
	c.Check(err, IsNil)
	c.Check(queues, DeepEquals, []string{"conn-q-q1"})

	queue2, err := connection.OpenQueue("conn-q-q2")
	c.Check(err, IsNil)
	c.Assert(queue2, NotNil)
	queues, err = connection.GetOpenQueues()
	c.Check(err, IsNil)
	c.Check(queues, HasLen, 2)
	queues, err = connection.getConsumingQueues()
	c.Check(err, IsNil)
	c.Check(queues, HasLen, 1)
	queue2.StartConsuming(1, time.Millisecond)
	queues, err = connection.getConsumingQueues()
	c.Check(err, IsNil)
	c.Check(queues, HasLen, 2)

	queue2.StopConsuming()
	queue2.closeInConnection()
	queues, err = connection.GetOpenQueues()
	c.Check(err, IsNil)
	c.Check(queues, HasLen, 2)
	queues, err = connection.getConsumingQueues()
	c.Check(err, IsNil)
	c.Check(queues, DeepEquals, []string{"conn-q-q1"})

	queue1.StopConsuming()
	queue1.closeInConnection()
	queues, err = connection.GetOpenQueues()
	c.Check(err, IsNil)
	c.Check(queues, HasLen, 2)
	queues, err = connection.getConsumingQueues()
	c.Check(err, IsNil)
	c.Check(queues, HasLen, 0)

	readyCount, rejectedCount, err := queue1.Destroy()
	c.Check(err, IsNil)
	c.Check(readyCount, Equals, int64(0))
	c.Check(rejectedCount, Equals, int64(0))
	queues, err = connection.GetOpenQueues()
	c.Check(err, IsNil)
	c.Check(queues, DeepEquals, []string{"conn-q-q2"})
	queues, err = connection.getConsumingQueues()
	c.Check(err, IsNil)
	c.Check(queues, HasLen, 0)

	connection.stopHeartbeat()
}

func (suite *QueueSuite) TestQueue(c *C) {
	connection, err := OpenConnection("queue-conn", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	c.Assert(connection, NotNil)

	queue, err := connection.OpenQueue("queue-q")
	c.Check(err, IsNil)
	c.Assert(queue, NotNil)
	_, err = queue.PurgeReady()
	c.Check(err, IsNil)
	count, err := queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	total, err := queue.Publish("queue-d1")
	c.Check(err, IsNil)
	c.Check(total, Equals, int64(1))
	count, err = queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(1))
	total, err = queue.Publish("queue-d2")
	c.Check(err, IsNil)
	c.Check(total, Equals, int64(2))
	count, err = queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(2))
	count, err = queue.PurgeReady()
	c.Check(count, Equals, int64(2))
	count, err = queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue.PurgeReady()
	c.Check(count, Equals, int64(0))

	queues, err := connection.getConsumingQueues()
	c.Check(err, IsNil)
	c.Check(queues, HasLen, 0)
	c.Check(queue.StartConsuming(10, time.Millisecond), IsNil)
	c.Check(queue.StartConsuming(10, time.Millisecond), Equals, ErrorAlreadyConsuming)
	cons1name, err := queue.AddConsumer("queue-cons1", NewTestConsumer("queue-A"))
	c.Check(err, IsNil)
	time.Sleep(time.Millisecond)
	queues, err = connection.getConsumingQueues()
	c.Check(err, IsNil)
	c.Check(queues, HasLen, 1)
	consumers, err := queue.getConsumers()
	c.Check(err, IsNil)
	c.Check(consumers, DeepEquals, []string{cons1name})
	_, err = queue.AddConsumer("queue-cons2", NewTestConsumer("queue-B"))
	c.Check(err, IsNil)
	consumers, err = queue.getConsumers()
	c.Check(err, IsNil)
	c.Check(consumers, HasLen, 2)

	queue.StopConsuming()
	connection.stopHeartbeat()
}

func (suite *QueueSuite) TestConsumer(c *C) {
	connection, err := OpenConnection("cons-conn", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	c.Assert(connection, NotNil)

	queue1, err := connection.OpenQueue("cons-q")
	c.Check(err, IsNil)
	c.Assert(queue1, NotNil)
	_, err = queue1.PurgeReady()
	c.Check(err, IsNil)

	consumer := NewTestConsumer("cons-A")
	consumer.AutoAck = false
	queue1.StartConsuming(10, time.Millisecond)
	_, err = queue1.AddConsumer("cons-cons", consumer)
	c.Check(err, IsNil)
	c.Check(consumer.LastDelivery, IsNil)

	total, err := queue1.Publish("cons-d1")
	c.Check(err, IsNil)
	c.Check(total, Equals, int64(1))
	time.Sleep(2 * time.Millisecond)
	c.Assert(consumer.LastDelivery, NotNil)
	c.Check(consumer.LastDelivery.Payload(), Equals, "cons-d1")
	count, err := queue1.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue1.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(1))

	total, err = queue1.Publish("cons-d2")
	c.Check(total, Equals, int64(1))
	c.Check(err, IsNil)
	time.Sleep(2 * time.Millisecond)
	c.Check(consumer.LastDelivery.Payload(), Equals, "cons-d2")
	count, err = queue1.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue1.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(2))

	err = consumer.LastDeliveries[0].Ack()
	c.Check(err, IsNil)
	count, err = queue1.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue1.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(1))

	err = consumer.LastDeliveries[1].Ack()
	c.Check(err, IsNil)
	count, err = queue1.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue1.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))

	err = consumer.LastDeliveries[0].Ack()
	c.Check(err, Equals, ErrorNotFound)

	total, err = queue1.Publish("cons-d3")
	c.Check(err, IsNil)
	c.Check(total, Equals, int64(1))
	time.Sleep(2 * time.Millisecond)
	count, err = queue1.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue1.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(1))
	count, err = queue1.rejectedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	c.Check(consumer.LastDelivery.Payload(), Equals, "cons-d3")
	err = consumer.LastDelivery.Reject()
	c.Check(err, IsNil)
	count, err = queue1.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue1.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue1.rejectedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(1))

	total, err = queue1.Publish("cons-d4")
	c.Check(err, IsNil)
	c.Check(total, Equals, int64(1))
	time.Sleep(2 * time.Millisecond)
	count, err = queue1.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue1.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(1))
	count, err = queue1.rejectedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(1))
	c.Check(consumer.LastDelivery.Payload(), Equals, "cons-d4")
	err = consumer.LastDelivery.Reject()
	c.Check(err, IsNil)
	count, err = queue1.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue1.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue1.rejectedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(2))
	count, err = queue1.PurgeRejected()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(2))
	count, err = queue1.rejectedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue1.PurgeRejected()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))

	queue2, err := connection.OpenQueue("cons-func-q")
	c.Check(err, IsNil)
	queue2.StartConsuming(10, time.Millisecond)

	payloadChan := make(chan string, 1)
	payload := "cons-func-payload"

	queue2.AddConsumerFunc("cons-func", func(delivery Delivery) {
		err = delivery.Ack()
		c.Check(err, IsNil)
		payloadChan <- delivery.Payload()
	})

	total, err = queue2.Publish(payload)
	c.Check(err, IsNil)
	c.Check(total, Equals, int64(1))
	time.Sleep(2 * time.Millisecond)
	c.Check(<-payloadChan, Equals, payload)
	count, err = queue2.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue2.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))

	queue1.StopConsuming()
	queue2.StopConsuming()
	connection.stopHeartbeat()
}

func (suite *QueueSuite) TestMulti(c *C) {
	connection, err := OpenConnection("multi-conn", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	queue, err := connection.OpenQueue("multi-q")
	c.Check(err, IsNil)
	_, err = queue.PurgeReady()
	c.Check(err, IsNil)

	for i := 0; i < 20; i++ {
		total, err := queue.Publish(fmt.Sprintf("multi-d%d", i))
		c.Check(err, IsNil)
		c.Check(total, Equals, int64(i+1))
	}
	count, err := queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(20))
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))

	queue.StartConsuming(10, time.Millisecond)
	time.Sleep(2 * time.Millisecond)
	count, err = queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(10))
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(10))

	consumer := NewTestConsumer("multi-cons")
	consumer.AutoAck = false
	consumer.AutoFinish = false

	_, err = queue.AddConsumer("multi-cons", consumer)
	c.Check(err, IsNil)
	time.Sleep(10 * time.Millisecond)
	count, err = queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(10))
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(10))

	err = consumer.LastDelivery.Ack()
	c.Check(err, IsNil)
	time.Sleep(10 * time.Millisecond)
	count, err = queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(9))
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(10))

	consumer.Finish()
	time.Sleep(10 * time.Millisecond)
	count, err = queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(9))
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(10))

	err = consumer.LastDelivery.Ack()
	c.Check(err, IsNil)
	time.Sleep(10 * time.Millisecond)
	count, err = queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(8))
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(10))

	consumer.Finish()
	time.Sleep(10 * time.Millisecond)
	count, err = queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(8))
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(10))

	queue.StopConsuming()
	connection.stopHeartbeat()
}

func (suite *QueueSuite) TestBatch(c *C) {
	connection, err := OpenConnection("batch-conn", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	queue, err := connection.OpenQueue("batch-q")
	c.Check(err, IsNil)
	_, err = queue.PurgeRejected()
	c.Check(err, IsNil)
	_, err = queue.PurgeReady()
	c.Check(err, IsNil)

	for i := 0; i < 5; i++ {
		total, err := queue.Publish(fmt.Sprintf("batch-d%d", i))
		c.Check(err, IsNil)
		c.Check(total, Equals, int64(i+1))
	}

	queue.StartConsuming(10, time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	count, err := queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(5))

	consumer := NewTestBatchConsumer()
	queue.AddBatchConsumerWithTimeout("batch-cons", 2, 50*time.Millisecond, consumer)
	time.Sleep(10 * time.Millisecond)
	c.Assert(consumer.LastBatch, HasLen, 2)
	c.Check(consumer.LastBatch[0].Payload(), Equals, "batch-d0")
	c.Check(consumer.LastBatch[1].Payload(), Equals, "batch-d1")
	err = consumer.LastBatch[0].Reject()
	c.Check(err, IsNil)
	err = consumer.LastBatch[1].Ack()
	c.Check(err, IsNil)
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(3))
	count, err = queue.rejectedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(1))

	consumer.Finish()
	time.Sleep(10 * time.Millisecond)
	c.Assert(consumer.LastBatch, HasLen, 2)
	c.Check(consumer.LastBatch[0].Payload(), Equals, "batch-d2")
	c.Check(consumer.LastBatch[1].Payload(), Equals, "batch-d3")
	err = consumer.LastBatch[0].Reject()
	c.Check(err, IsNil)
	err = consumer.LastBatch[1].Ack()
	c.Check(err, IsNil)
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(1))
	count, err = queue.rejectedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(2))

	consumer.Finish()
	time.Sleep(10 * time.Millisecond)
	c.Check(consumer.LastBatch, HasLen, 0)
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(1))
	count, err = queue.rejectedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(2))

	time.Sleep(60 * time.Millisecond)
	c.Assert(consumer.LastBatch, HasLen, 1)
	c.Check(consumer.LastBatch[0].Payload(), Equals, "batch-d4")
	err = consumer.LastBatch[0].Reject()
	c.Check(err, IsNil)
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue.rejectedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(3))
}

func (suite *QueueSuite) TestReturnRejected(c *C) {
	connection, err := OpenConnection("return-conn", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	queue, err := connection.OpenQueue("return-q")
	c.Check(err, IsNil)
	_, err = queue.PurgeReady()
	c.Check(err, IsNil)

	for i := 0; i < 6; i++ {
		total, err := queue.Publish(fmt.Sprintf("return-d%d", i))
		c.Check(err, IsNil)
		c.Check(total, Equals, int64(i+1))
	}

	count, err := queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(6))
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue.rejectedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))

	queue.StartConsuming(10, time.Millisecond)
	time.Sleep(time.Millisecond)
	count, err = queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(6))
	count, err = queue.rejectedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))

	consumer := NewTestConsumer("return-cons")
	consumer.AutoAck = false
	_, err = queue.AddConsumer("cons", consumer)
	c.Check(err, IsNil)
	time.Sleep(time.Millisecond)
	count, err = queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(6))
	count, err = queue.rejectedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))

	c.Check(consumer.LastDeliveries, HasLen, 6)
	err = consumer.LastDeliveries[0].Reject()
	c.Check(err, IsNil)
	err = consumer.LastDeliveries[1].Ack()
	c.Check(err, IsNil)
	err = consumer.LastDeliveries[2].Reject()
	c.Check(err, IsNil)
	err = consumer.LastDeliveries[3].Reject()
	c.Check(err, IsNil)
	// delivery 4 still open
	err = consumer.LastDeliveries[5].Reject()
	c.Check(err, IsNil)

	time.Sleep(time.Millisecond)
	count, err = queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(1)) // delivery 4
	count, err = queue.rejectedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(4)) // delivery 0, 2, 3, 5

	queue.StopConsuming()

	queue.ReturnRejected(2)
	count, err = queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(2)) // delivery 0, 2
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(1)) // delivery 4
	count, err = queue.rejectedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(2)) // delivery 3, 5

	queue.ReturnAllRejected()
	count, err = queue.readyCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(4)) // delivery 0, 2, 3, 5
	count, err = queue.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(1)) // delivery 4
	count, err = queue.rejectedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
}

func (suite *QueueSuite) TestPushQueue(c *C) {
	connection, err := OpenConnection("push", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	queue1, err := connection.OpenQueue("queue1")
	c.Check(err, IsNil)
	queue2, err := connection.OpenQueue("queue2")
	c.Check(err, IsNil)
	queue1.SetPushQueue(queue2)
	c.Check(queue1.(*redisQueue).pushKey, Equals, queue2.(*redisQueue).readyKey)

	consumer1 := NewTestConsumer("push-cons")
	consumer1.AutoAck = false
	consumer1.AutoFinish = false
	queue1.StartConsuming(10, time.Millisecond)
	_, err = queue1.AddConsumer("push-cons", consumer1)
	c.Check(err, IsNil)

	consumer2 := NewTestConsumer("push-cons")
	consumer2.AutoAck = false
	consumer2.AutoFinish = false
	queue2.StartConsuming(10, time.Millisecond)
	_, err = queue2.AddConsumer("push-cons", consumer2)
	c.Check(err, IsNil)

	total, err := queue1.Publish("d1")
	c.Check(err, IsNil)
	c.Check(total, Equals, int64(1))
	time.Sleep(2 * time.Millisecond)
	count, err := queue1.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(1))
	c.Assert(consumer1.LastDeliveries, HasLen, 1)

	err = consumer1.LastDelivery.Push()
	c.Check(err, IsNil)
	time.Sleep(2 * time.Millisecond)
	count, err = queue1.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(0))
	count, err = queue2.unackedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(1))

	c.Assert(consumer2.LastDeliveries, HasLen, 1)
	err = consumer2.LastDelivery.Push()
	c.Check(err, IsNil)
	time.Sleep(2 * time.Millisecond)
	count, err = queue2.rejectedCount()
	c.Check(err, IsNil)
	c.Check(count, Equals, int64(1))
}

func (suite *QueueSuite) TestConsuming(c *C) {
	connection, err := OpenConnection("consume", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	queue, err := connection.OpenQueue("consume-q")
	c.Check(err, IsNil)

	finishedChan := queue.StopConsuming()
	c.Check(finishedChan, NotNil)
	select {
	case <-finishedChan:
	default:
		c.FailNow() // should return closed finishedChan
	}

	queue.StartConsuming(10, time.Millisecond)
	c.Check(queue.StopConsuming(), NotNil)
	// already stopped
	c.Check(queue.StopConsuming(), NotNil)
	select {
	case <-finishedChan:
	default:
		c.FailNow() // should return closed finishedChan
	}
}

func (suite *QueueSuite) TestStopConsuming_Consumer(c *C) {
	connection, err := OpenConnection("consume", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	queue, err := connection.OpenQueue("consume-q")
	c.Check(err, IsNil)
	_, err = queue.PurgeReady()
	c.Check(err, IsNil)

	deliveryCount := int64(30)

	for i := int64(0); i < deliveryCount; i++ {
		total, err := queue.Publish("d" + strconv.FormatInt(i, 10))
		c.Check(err, IsNil)
		c.Check(total, Equals, i+1)
	}

	queue.StartConsuming(20, time.Millisecond)
	var consumers []*TestConsumer
	for i := 0; i < 10; i++ {
		consumer := NewTestConsumer("c" + strconv.Itoa(i))
		consumers = append(consumers, consumer)
		_, err = queue.AddConsumer("consume", consumer)
		c.Check(err, IsNil)
	}

	finishedChan := queue.StopConsuming()
	c.Assert(finishedChan, NotNil)

	<-finishedChan

	var consumedCount int64
	for i := 0; i < 10; i++ {
		consumedCount += int64(len(consumers[i].LastDeliveries))
	}

	// make sure all fetched deliveries are consumed
	readyCount, err := queue.readyCount()
	c.Check(err, IsNil)
	count := deliveryCount - readyCount
	c.Check(consumedCount, Equals, count)
	c.Check(queue.(*redisQueue).deliveryChan, HasLen, 0)

	connection.stopHeartbeat()
}

func (suite *QueueSuite) TestStopConsuming_BatchConsumer(c *C) {
	connection, err := OpenConnection("batchConsume", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	queue, err := connection.OpenQueue("batchConsume-q")
	c.Check(err, IsNil)
	_, err = queue.PurgeReady()
	c.Check(err, IsNil)

	deliveryCount := int64(50)

	for i := int64(0); i < deliveryCount; i++ {
		total, err := queue.Publish("d" + strconv.FormatInt(i, 10))
		c.Check(err, IsNil)
		c.Check(total, Equals, i+1)
	}

	queue.StartConsuming(20, time.Millisecond)

	var consumers []*TestBatchConsumer
	for i := 0; i < 10; i++ {
		consumer := NewTestBatchConsumer()
		consumer.AutoFinish = true
		consumers = append(consumers, consumer)
		queue.AddBatchConsumer("consume", 5, consumer)
	}
	consumer := NewTestBatchConsumer()
	consumer.AutoFinish = true

	finishedChan := queue.StopConsuming()
	c.Assert(finishedChan, NotNil)

	<-finishedChan

	var consumedCount int64
	for i := 0; i < 10; i++ {
		consumedCount += consumers[i].ConsumedCount
	}

	// make sure all fetched deliveries are consumed
	readyCount, err := queue.readyCount()
	c.Check(err, IsNil)
	count := deliveryCount - readyCount
	c.Check(err, IsNil)
	c.Check(consumedCount, Equals, count)
	c.Check(queue.(*redisQueue).deliveryChan, HasLen, 0)

	connection.stopHeartbeat()
}

func (suite *QueueSuite) BenchmarkQueue(c *C) {
	// open queue
	connection, err := OpenConnection("bench-conn", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	queueName := fmt.Sprintf("bench-q%d", c.N)
	queue, err := connection.OpenQueue(queueName)
	c.Check(err, IsNil)

	// add some consumers
	numConsumers := 10
	var consumers []*TestConsumer
	for i := 0; i < numConsumers; i++ {
		consumer := NewTestConsumer("bench-A")
		// consumer.SleepDuration = time.Microsecond
		consumers = append(consumers, consumer)
		queue.StartConsuming(10, time.Millisecond)
		_, err = queue.AddConsumer("bench-cons", consumer)
		c.Check(err, IsNil)
	}

	// publish deliveries
	for i := 0; i < c.N; i++ {
		total, err := queue.Publish("bench-d")
		c.Check(err, IsNil)
		c.Check(total, Equals, int64(i))
	}

	// wait until all are consumed
	for {
		ready, err := queue.readyCount()
		c.Check(err, IsNil)
		unacked, err := queue.unackedCount()
		c.Check(err, IsNil)
		fmt.Printf("%d unacked %d %d\n", c.N, ready, unacked)
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

	connection.stopHeartbeat()
}
