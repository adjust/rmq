package rmq

import (
	"fmt"
	"testing"
	"time"

	. "github.com/adjust/gocheck"
)

func TestQueueSuite(t *testing.T) {
	TestingSuiteT(&QueueSuite{}, t)
}

type QueueSuite struct{}

func (suite *QueueSuite) TestConnections(c *C) {
	flushConn := OpenConnection("conns-flush", "tcp", "localhost:6379", 1)
	flushConn.flushDb()
	flushConn.StopHeartbeat()

	connection := OpenConnection("conns-conn", "tcp", "localhost:6379", 1)
	c.Assert(connection, NotNil)
	c.Assert(NewCleaner(connection).Clean(), IsNil)

	c.Check(connection.GetConnections(), HasLen, 1, Commentf("cleaner %s", connection.Name)) // cleaner connection remains

	conn1 := OpenConnection("conns-conn1", "tcp", "localhost:6379", 1)
	c.Check(connection.GetConnections(), HasLen, 2)
	c.Check(connection.hijackConnection("nope").Check(), Equals, false)
	c.Check(conn1.Check(), Equals, true)
	conn2 := OpenConnection("conns-conn2", "tcp", "localhost:6379", 1)
	c.Check(connection.GetConnections(), HasLen, 3)
	c.Check(conn1.Check(), Equals, true)
	c.Check(conn2.Check(), Equals, true)

	connection.hijackConnection("nope").StopHeartbeat()
	conn1.StopHeartbeat()
	c.Check(conn1.Check(), Equals, false)
	c.Check(conn2.Check(), Equals, true)
	c.Check(connection.GetConnections(), HasLen, 3)

	conn2.StopHeartbeat()
	c.Check(conn1.Check(), Equals, false)
	c.Check(conn2.Check(), Equals, false)
	c.Check(connection.GetConnections(), HasLen, 3)

	connection.StopHeartbeat()
}

func (suite *QueueSuite) TestConnectionQueues(c *C) {
	connection := OpenConnection("conn-q-conn", "tcp", "localhost:6379", 1)
	c.Assert(connection, NotNil)

	connection.CloseAllQueues()
	c.Check(connection.GetOpenQueues(), HasLen, 0)

	queue1 := connection.OpenQueue("conn-q-q1").(*redisQueue)
	c.Assert(queue1, NotNil)
	c.Check(connection.GetOpenQueues(), DeepEquals, []string{"conn-q-q1"})
	c.Check(connection.GetConsumingQueues(), HasLen, 0)
	queue1.StartConsuming(1, time.Millisecond)
	c.Check(connection.GetConsumingQueues(), DeepEquals, []string{"conn-q-q1"})

	queue2 := connection.OpenQueue("conn-q-q2").(*redisQueue)
	c.Assert(queue2, NotNil)
	c.Check(connection.GetOpenQueues(), HasLen, 2)
	c.Check(connection.GetConsumingQueues(), HasLen, 1)
	queue2.StartConsuming(1, time.Millisecond)
	c.Check(connection.GetConsumingQueues(), HasLen, 2)

	queue2.StopConsuming()
	queue2.CloseInConnection()
	c.Check(connection.GetOpenQueues(), HasLen, 2)
	c.Check(connection.GetConsumingQueues(), DeepEquals, []string{"conn-q-q1"})

	queue1.StopConsuming()
	queue1.CloseInConnection()
	c.Check(connection.GetOpenQueues(), HasLen, 2)
	c.Check(connection.GetConsumingQueues(), HasLen, 0)

	queue1.Close()
	c.Check(connection.GetOpenQueues(), DeepEquals, []string{"conn-q-q2"})
	c.Check(connection.GetConsumingQueues(), HasLen, 0)

	connection.StopHeartbeat()
}

func (suite *QueueSuite) TestQueue(c *C) {
	connection := OpenConnection("queue-conn", "tcp", "localhost:6379", 1)
	c.Assert(connection, NotNil)

	queue := connection.OpenQueue("queue-q").(*redisQueue)
	c.Assert(queue, NotNil)
	queue.PurgeReady()
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.Publish("queue-d1"), Equals, true)
	c.Check(queue.ReadyCount(), Equals, 1)
	c.Check(queue.Publish("queue-d2"), Equals, true)
	c.Check(queue.ReadyCount(), Equals, 2)
	c.Check(queue.PurgeReady(), Equals, true)
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.PurgeReady(), Equals, false)

	queue.RemoveAllConsumers()
	c.Check(queue.GetConsumers(), HasLen, 0)
	c.Check(connection.GetConsumingQueues(), HasLen, 0)
	c.Check(queue.StartConsuming(10, time.Millisecond), Equals, true)
	c.Check(queue.StartConsuming(10, time.Millisecond), Equals, false)
	cons1name, _ := queue.AddConsumer("queue-cons1", NewTestConsumer("queue-A"))
	time.Sleep(time.Millisecond)
	c.Check(connection.GetConsumingQueues(), HasLen, 1)
	c.Check(queue.GetConsumers(), DeepEquals, []string{cons1name})
	cons2name, _ := queue.AddConsumer("queue-cons2", NewTestConsumer("queue-B"))
	c.Check(queue.GetConsumers(), HasLen, 2)
	c.Check(queue.RemoveConsumer("queue-cons3"), Equals, false)
	c.Check(queue.RemoveConsumer(cons1name), Equals, true)
	c.Check(queue.GetConsumers(), DeepEquals, []string{cons2name})
	c.Check(queue.RemoveConsumer(cons2name), Equals, true)
	c.Check(queue.GetConsumers(), HasLen, 0)

	queue.StopConsuming()
	connection.StopHeartbeat()
}

func (suite *QueueSuite) TestConsumer(c *C) {
	connection := OpenConnection("cons-conn", "tcp", "localhost:6379", 1)
	c.Assert(connection, NotNil)

	queue := connection.OpenQueue("cons-q").(*redisQueue)
	c.Assert(queue, NotNil)
	queue.PurgeReady()

	consumer := NewTestConsumer("cons-A")
	consumer.AutoAck = false
	queue.StartConsuming(10, time.Millisecond)
	queue.AddConsumer("cons-cons", consumer)
	c.Check(consumer.LastDelivery, IsNil)

	c.Check(queue.Publish("cons-d1"), Equals, true)
	time.Sleep(2 * time.Millisecond)
	c.Assert(consumer.LastDelivery, NotNil)
	c.Check(consumer.LastDelivery.Payload(), Equals, "cons-d1")
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 1)

	c.Check(queue.Publish("cons-d2"), Equals, true)
	time.Sleep(2 * time.Millisecond)
	c.Check(consumer.LastDelivery.Payload(), Equals, "cons-d2")
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 2)

	c.Check(consumer.LastDeliveries[0].Ack(), Equals, true)
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 1)

	c.Check(consumer.LastDeliveries[1].Ack(), Equals, true)
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 0)

	c.Check(consumer.LastDeliveries[0].Ack(), Equals, false)

	c.Check(queue.Publish("cons-d3"), Equals, true)
	time.Sleep(2 * time.Millisecond)
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 1)
	c.Check(queue.RejectedCount(), Equals, 0)
	c.Check(consumer.LastDelivery.Payload(), Equals, "cons-d3")
	c.Check(consumer.LastDelivery.Reject(), Equals, true)
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 0)
	c.Check(queue.RejectedCount(), Equals, 1)

	c.Check(queue.Publish("cons-d4"), Equals, true)
	time.Sleep(2 * time.Millisecond)
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 1)
	c.Check(queue.RejectedCount(), Equals, 1)
	c.Check(consumer.LastDelivery.Payload(), Equals, "cons-d4")
	c.Check(consumer.LastDelivery.Reject(), Equals, true)
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 0)
	c.Check(queue.RejectedCount(), Equals, 2)

	c.Check(queue.PurgeRejected(), Equals, true)
	c.Check(queue.RejectedCount(), Equals, 0)
	c.Check(queue.PurgeRejected(), Equals, false)

	queue.StopConsuming()
	connection.StopHeartbeat()
}

func (suite *QueueSuite) TestMulti(c *C) {
	connection := OpenConnection("multi-conn", "tcp", "localhost:6379", 1)
	queue := connection.OpenQueue("multi-q").(*redisQueue)
	queue.PurgeReady()

	for i := 0; i < 20; i++ {
		c.Check(queue.Publish(fmt.Sprintf("multi-d%d", i)), Equals, true)
	}
	c.Check(queue.ReadyCount(), Equals, 20)
	c.Check(queue.UnackedCount(), Equals, 0)

	queue.StartConsuming(10, time.Millisecond)
	time.Sleep(2 * time.Millisecond)
	c.Check(queue.ReadyCount(), Equals, 10)
	c.Check(queue.UnackedCount(), Equals, 10)

	consumer := NewTestConsumer("multi-cons")
	consumer.AutoAck = false
	consumer.AutoFinish = false

	queue.AddConsumer("multi-cons", consumer)
	time.Sleep(2 * time.Millisecond)
	c.Check(queue.ReadyCount(), Equals, 9)
	c.Check(queue.UnackedCount(), Equals, 11)

	c.Check(consumer.LastDelivery.Ack(), Equals, true)
	time.Sleep(2 * time.Millisecond)
	c.Check(queue.ReadyCount(), Equals, 9)
	c.Check(queue.UnackedCount(), Equals, 10)

	consumer.Finish()
	time.Sleep(2 * time.Millisecond)
	c.Check(queue.ReadyCount(), Equals, 8)
	c.Check(queue.UnackedCount(), Equals, 11)

	c.Check(consumer.LastDelivery.Ack(), Equals, true)
	time.Sleep(2 * time.Millisecond)
	c.Check(queue.ReadyCount(), Equals, 8)
	c.Check(queue.UnackedCount(), Equals, 10)

	consumer.Finish()
	time.Sleep(2 * time.Millisecond)
	c.Check(queue.ReadyCount(), Equals, 7)
	c.Check(queue.UnackedCount(), Equals, 11)

	queue.StopConsuming()
	connection.StopHeartbeat()
}

func (suite *QueueSuite) TestStop(c *C) {
	connection := OpenConnection("stop-conn", "tcp", "localhost:6379", 1)
	queue := connection.OpenQueue("stop-q").(*redisQueue)
	queue.PurgeRejected()
	queue.PurgeReady()
	consumer := NewTestConsumer("stop-cons")

	queue.StartConsuming(10, time.Millisecond)
	_, context := queue.AddConsumer("stop-cons", consumer)

	c.Check(queue.Publish("stop-d1"), Equals, true)
	time.Sleep(2 * time.Millisecond)
	c.Check(consumer.LastDeliveries, HasLen, 1)
	c.Check(consumer.LastDelivery.Payload(), Equals, "stop-d1")

	context.StopChan <- 1

	c.Check(queue.Publish("stop-d2"), Equals, true)
	time.Sleep(2 * time.Millisecond)
	c.Check(consumer.LastDeliveries, HasLen, 1)
	c.Check(consumer.LastDelivery.Payload(), Equals, "stop-d1")
}

func (suite *QueueSuite) TestBatch(c *C) {
	connection := OpenConnection("batch-conn", "tcp", "localhost:6379", 1)
	queue := connection.OpenQueue("batch-q").(*redisQueue)
	queue.PurgeRejected()
	queue.PurgeReady()

	for i := 0; i < 5; i++ {
		c.Check(queue.Publish(fmt.Sprintf("batch-d%d", i)), Equals, true)
	}

	queue.StartConsuming(10, time.Millisecond)
	time.Sleep(2 * time.Millisecond)
	c.Check(queue.UnackedCount(), Equals, 5)

	consumer := NewTestBatchConsumer()
	queue.AddBatchConsumer("batch-cons", 2, consumer)
	time.Sleep(2 * time.Millisecond)
	c.Assert(consumer.LastBatch, HasLen, 2)
	c.Check(consumer.LastBatch[0].Payload(), Equals, "batch-d0")
	c.Check(consumer.LastBatch[1].Payload(), Equals, "batch-d1")
	c.Check(consumer.LastBatch[0].Reject(), Equals, true)
	c.Check(consumer.LastBatch[1].Ack(), Equals, true)
	c.Check(queue.UnackedCount(), Equals, 3)
	c.Check(queue.RejectedCount(), Equals, 1)

	consumer.Finish()
	time.Sleep(2 * time.Millisecond)
	c.Assert(consumer.LastBatch, HasLen, 2)
	c.Check(consumer.LastBatch[0].Payload(), Equals, "batch-d2")
	c.Check(consumer.LastBatch[1].Payload(), Equals, "batch-d3")
	c.Check(consumer.LastBatch[0].Reject(), Equals, true)
	c.Check(consumer.LastBatch[1].Ack(), Equals, true)
	c.Check(queue.UnackedCount(), Equals, 1)
	c.Check(queue.RejectedCount(), Equals, 2)

	consumer.Finish()
	time.Sleep(2 * time.Millisecond)
	c.Check(consumer.LastBatch, HasLen, 0)
	c.Check(queue.UnackedCount(), Equals, 1)
	c.Check(queue.RejectedCount(), Equals, 2)

	c.Check(queue.Publish("batch-d5"), Equals, true)
	time.Sleep(2 * time.Millisecond)
	c.Assert(consumer.LastBatch, HasLen, 2)
	c.Check(consumer.LastBatch[0].Payload(), Equals, "batch-d4")
	c.Check(consumer.LastBatch[1].Payload(), Equals, "batch-d5")
	c.Check(consumer.LastBatch[0].Reject(), Equals, true)
	c.Check(consumer.LastBatch[1].Ack(), Equals, true)
	c.Check(queue.UnackedCount(), Equals, 0)
	c.Check(queue.RejectedCount(), Equals, 3)
}

func (suite *QueueSuite) TestReturnRejected(c *C) {
	connection := OpenConnection("return-conn", "tcp", "localhost:6379", 1)
	queue := connection.OpenQueue("return-q").(*redisQueue)
	queue.PurgeReady()

	for i := 0; i < 6; i++ {
		c.Check(queue.Publish(fmt.Sprintf("return-d%d", i)), Equals, true)
	}

	c.Check(queue.ReadyCount(), Equals, 6)
	c.Check(queue.UnackedCount(), Equals, 0)
	c.Check(queue.RejectedCount(), Equals, 0)

	queue.StartConsuming(10, time.Millisecond)
	time.Sleep(time.Millisecond)
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 6)
	c.Check(queue.RejectedCount(), Equals, 0)

	consumer := NewTestConsumer("return-cons")
	consumer.AutoAck = false
	queue.AddConsumer("cons", consumer)
	time.Sleep(time.Millisecond)
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 6)
	c.Check(queue.RejectedCount(), Equals, 0)

	c.Check(consumer.LastDeliveries, HasLen, 6)
	consumer.LastDeliveries[0].Reject()
	consumer.LastDeliveries[1].Ack()
	consumer.LastDeliveries[2].Reject()
	consumer.LastDeliveries[3].Reject()
	// delivery 4 still open
	consumer.LastDeliveries[5].Reject()

	time.Sleep(time.Millisecond)
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 1)  // delivery 4
	c.Check(queue.RejectedCount(), Equals, 4) // delivery 0, 2, 3, 5

	queue.StopConsuming()

	queue.ReturnRejected(2)
	c.Check(queue.ReadyCount(), Equals, 2)    // delivery 0, 2
	c.Check(queue.UnackedCount(), Equals, 1)  // delivery 4
	c.Check(queue.RejectedCount(), Equals, 2) // delivery 3, 5

	queue.ReturnAllRejected()
	c.Check(queue.ReadyCount(), Equals, 4)   // delivery 0, 2, 3, 5
	c.Check(queue.UnackedCount(), Equals, 1) // delivery 4
	c.Check(queue.RejectedCount(), Equals, 0)
}

func (suite *QueueSuite) TestPushQueue(c *C) {
	connection := OpenConnection("push", "tcp", "localhost:6379", 1)
	queue1 := connection.OpenQueue("queue1").(*redisQueue)
	queue2 := connection.OpenQueue("queue2").(*redisQueue)
	queue1.SetPushQueue(queue2)
	c.Check(queue1.pushKey, Equals, queue2.readyKey)

	consumer1 := NewTestConsumer("push-cons")
	consumer1.AutoAck = false
	consumer1.AutoFinish = false
	queue1.StartConsuming(10, time.Millisecond)
	queue1.AddConsumer("push-cons", consumer1)

	consumer2 := NewTestConsumer("push-cons")
	consumer2.AutoAck = false
	consumer2.AutoFinish = false
	queue2.StartConsuming(10, time.Millisecond)
	queue2.AddConsumer("push-cons", consumer2)

	queue1.Publish("d1")
	time.Sleep(2 * time.Millisecond)
	c.Check(queue1.UnackedCount(), Equals, 1)
	c.Assert(consumer1.LastDeliveries, HasLen, 1)

	c.Check(consumer1.LastDelivery.Push(), Equals, true)
	time.Sleep(2 * time.Millisecond)
	c.Check(queue1.UnackedCount(), Equals, 0)
	c.Check(queue2.UnackedCount(), Equals, 1)

	c.Assert(consumer2.LastDeliveries, HasLen, 1)
	c.Check(consumer2.LastDelivery.Push(), Equals, true)
	time.Sleep(2 * time.Millisecond)
	c.Check(queue2.RejectedCount(), Equals, 1)
}

func (suite *QueueSuite) TestConsuming(c *C) {
	connection := OpenConnection("consume", "tcp", "localhost:6379", 1)
	queue := connection.OpenQueue("consume-q").(*redisQueue)

	c.Check(queue.StopConsuming(), Equals, false)

	queue.StartConsuming(10, time.Millisecond)
	c.Check(queue.StopConsuming(), Equals, true)
	c.Check(queue.StopConsuming(), Equals, false)
}

func (suite *QueueSuite) BenchmarkQueue(c *C) {
	// open queue
	connection := OpenConnection("bench-conn", "tcp", "localhost:6379", 1)
	queueName := fmt.Sprintf("bench-q%d", c.N)
	queue := connection.OpenQueue(queueName).(*redisQueue)

	// add some consumers
	numConsumers := 10
	var consumers []*TestConsumer
	for i := 0; i < numConsumers; i++ {
		consumer := NewTestConsumer("bench-A")
		// consumer.SleepDuration = time.Microsecond
		consumers = append(consumers, consumer)
		queue.StartConsuming(10, time.Millisecond)
		queue.AddConsumer("bench-cons", consumer)
	}

	// publish deliveries
	for i := 0; i < c.N; i++ {
		c.Check(queue.Publish("bench-d"), Equals, true)
	}

	// wait until all are consumed
	for {
		ready := queue.ReadyCount()
		unacked := queue.UnackedCount()
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

	connection.StopHeartbeat()
}
