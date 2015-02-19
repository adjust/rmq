package queue

import (
	"fmt"
	"testing"
	"time"

	"github.com/adjust/goenv"

	. "github.com/adjust/gocheck"
)

func TestQueueSuite(t *testing.T) {
	TestingSuiteT(&QueueSuite{}, t)
}

type QueueSuite struct {
	goenv *goenv.Goenv
}

func (suite *QueueSuite) SetUpSuite(c *C) {
	suite.goenv = goenv.TestGoenv()
}

func (suite *QueueSuite) TestConnections(c *C) {
	host, port, db := suite.goenv.GetRedis()
	connection := OpenConnection("conns-conn", host, port, db)
	c.Assert(connection, NotNil)
	c.Assert(NewCleaner(connection).Clean(), IsNil)

	c.Check(connection.GetConnections(), HasLen, 1, Commentf("cleaner %s", connection.Name)) // cleaner connection remains

	conn1 := OpenConnection("conns-conn1", host, port, db)
	c.Check(connection.GetConnections(), HasLen, 2)
	c.Check(connection.hijackConnection("nope").Check(), Equals, false)
	c.Check(conn1.Check(), Equals, true)
	conn2 := OpenConnection("conns-conn2", host, port, db)
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
	host, port, db := suite.goenv.GetRedis()
	connection := OpenConnection("conn-q-conn", host, port, db)
	c.Assert(connection, NotNil)

	connection.CloseAllQueues()
	c.Check(connection.GetOpenQueues(), HasLen, 0)

	c.Assert(connection.OpenQueue("conn-q-q1"), NotNil)
	c.Check(connection.GetOpenQueues(), DeepEquals, []string{"conn-q-q1"})

	c.Assert(connection.OpenQueue("conn-q-q2"), NotNil)
	c.Check(connection.GetOpenQueues(), HasLen, 2)

	c.Check(connection.CloseQueue("conn-q-q3"), Equals, false)

	c.Check(connection.CloseQueue("conn-q-q2"), Equals, true)
	c.Check(connection.GetOpenQueues(), DeepEquals, []string{"conn-q-q1"})

	c.Check(connection.CloseQueue("conn-q-q1"), Equals, true)
	c.Check(connection.GetOpenQueues(), HasLen, 0)

	connection.StopHeartbeat()
}

func (suite *QueueSuite) TestQueue(c *C) {
	host, port, db := suite.goenv.GetRedis()
	connection := OpenConnection("queue-conn", host, port, db)
	c.Assert(connection, NotNil)

	queue := connection.OpenQueue("queue-q")
	c.Assert(queue, NotNil)
	queue.Clear()
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.Publish("queue-d1"), IsNil)
	c.Check(queue.ReadyCount(), Equals, 1)
	c.Check(queue.Publish("queue-d2"), IsNil)
	c.Check(queue.ReadyCount(), Equals, 2)
	c.Check(queue.Purge(), Equals, true)
	c.Check(queue.ReadyCount(), Equals, 0)

	queue.RemoveAllConsumers()
	c.Check(queue.GetConsumers(), HasLen, 0)
	c.Check(connection.GetConsumingQueues(), HasLen, 0)
	c.Check(queue.StartConsuming(10), Equals, true)
	c.Check(queue.StartConsuming(10), Equals, false)
	cons1name := queue.AddConsumer("queue-cons1", NewTestConsumer())
	time.Sleep(time.Millisecond)
	c.Check(connection.GetConsumingQueues(), HasLen, 1)
	c.Check(queue.GetConsumers(), DeepEquals, []string{cons1name})
	cons2name := queue.AddConsumer("queue-cons2", NewTestConsumer())
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
	host, port, db := suite.goenv.GetRedis()
	connection := OpenConnection("cons-conn", host, port, db)
	c.Assert(connection, NotNil)

	queue := connection.OpenQueue("cons-q")
	c.Assert(queue, NotNil)
	queue.Clear()

	consumer := NewTestConsumer()
	queue.StartConsuming(10)
	queue.AddConsumer("cons-cons", consumer)
	c.Check(consumer.LastDelivery, IsNil)

	c.Check(queue.Publish("cons-d1"), IsNil)
	time.Sleep(2 * time.Millisecond)
	c.Assert(consumer.LastDelivery, NotNil)
	c.Check(consumer.LastDelivery.Payload(), Equals, "cons-d1")
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 1)

	c.Check(queue.Publish("cons-d2"), IsNil)
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

	queue.StopConsuming()
	connection.StopHeartbeat()
}

func (suite *QueueSuite) BenchmarkQueue(c *C) {
	// open queue
	host, port, db := suite.goenv.GetRedis()
	connection := OpenConnection("bench-conn", host, port, db)
	queueName := fmt.Sprintf("bench-q%d", c.N)
	queue := connection.OpenQueue(queueName)

	// add some consumers
	numConsumers := 10
	var consumers []*TestConsumer
	for i := 0; i < numConsumers; i++ {
		consumer := NewTestConsumer()
		consumer.AutoAck = true
		consumers = append(consumers, consumer)
		queue.StartConsuming(10)
		queue.AddConsumer("bench-cons", consumer)
	}

	// publish deliveries
	for i := 0; i < c.N; i++ {
		queue.Publish("bench-d")
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
