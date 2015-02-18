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
	connection := OpenConnection("test", host, port, db)
	c.Assert(connection, NotNil)

	connection.CloseAllConnections()
	c.Check(connection.GetConnections(), HasLen, 0)

	conn1 := OpenConnection("test1", host, port, db)
	c.Check(connection.GetConnections(), DeepEquals, []string{conn1.Name})
	c.Check(connection.hijackConnection("nope").Check(), Equals, false)
	c.Check(conn1.Check(), Equals, true)
	conn2 := OpenConnection("test2", host, port, db)
	c.Check(connection.GetConnections(), HasLen, 2)
	c.Check(conn1.Check(), Equals, true)
	c.Check(conn2.Check(), Equals, true)

	connection.hijackConnection("nope").StopHeartbeat()
	conn1.StopHeartbeat()
	c.Check(conn1.Check(), Equals, false)
	c.Check(conn2.Check(), Equals, true)
	c.Check(connection.GetConnections(), HasLen, 2)

	conn2.StopHeartbeat()
	c.Check(conn1.Check(), Equals, false)
	c.Check(conn2.Check(), Equals, false)
	c.Check(connection.GetConnections(), HasLen, 2)
}

func (suite *QueueSuite) TestConnectionQueues(c *C) {
	host, port, db := suite.goenv.GetRedis()
	connection := OpenConnection("test", host, port, db)
	c.Assert(connection, NotNil)

	connection.CloseAllQueues()
	c.Check(connection.GetOpenQueues(), HasLen, 0)

	c.Assert(connection.OpenQueue("things"), NotNil)
	c.Check(connection.GetOpenQueues(), DeepEquals, []string{"things"})

	c.Assert(connection.OpenQueue("balls"), NotNil)
	c.Check(connection.GetOpenQueues(), DeepEquals, []string{"balls", "things"})

	c.Check(connection.CloseQueue("apples"), Equals, false)

	c.Check(connection.CloseQueue("balls"), Equals, true)
	c.Check(connection.GetOpenQueues(), DeepEquals, []string{"things"})

	c.Check(connection.CloseQueue("things"), Equals, true)
	c.Check(connection.GetOpenQueues(), HasLen, 0)
}

func (suite *QueueSuite) TestQueue(c *C) {
	host, port, db := suite.goenv.GetRedis()
	connection := OpenConnection("test", host, port, db)
	c.Assert(connection, NotNil)

	queue := connection.OpenQueue("things")
	c.Assert(queue, NotNil)
	queue.Clear()
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.Publish("test"), IsNil)
	c.Check(queue.ReadyCount(), Equals, 1)
	c.Check(queue.Publish("test"), IsNil)
	c.Check(queue.ReadyCount(), Equals, 2)
	c.Check(queue.Purge(), Equals, true)
	c.Check(queue.ReadyCount(), Equals, 0)

	queue.RemoveAllConsumers()
	c.Check(queue.GetConsumers(), HasLen, 0)
	c.Check(connection.GetConsumingQueues(), HasLen, 0)
	c.Check(queue.PrepareConsumption(10), Equals, true)
	c.Check(queue.PrepareConsumption(10), Equals, false)
	nameTest := queue.AddConsumer("test", NewTestConsumer())
	time.Sleep(time.Millisecond)
	c.Check(connection.GetConsumingQueues(), HasLen, 1)
	c.Check(queue.GetConsumers(), DeepEquals, []string{nameTest})
	nameFoo := queue.AddConsumer("foo", NewTestConsumer())
	c.Check(queue.GetConsumers(), HasLen, 2)
	c.Check(queue.RemoveConsumer("nope"), Equals, false)
	c.Check(queue.RemoveConsumer(nameTest), Equals, true)
	c.Check(queue.GetConsumers(), DeepEquals, []string{nameFoo})
	c.Check(queue.RemoveConsumer(nameFoo), Equals, true)
	c.Check(queue.GetConsumers(), HasLen, 0)
}

func (suite *QueueSuite) TestConsumer(c *C) {
	host, port, db := suite.goenv.GetRedis()
	connection := OpenConnection("test", host, port, db)
	c.Assert(connection, NotNil)

	queue := connection.OpenQueue("things3")
	c.Assert(queue, NotNil)
	queue.Clear()

	consumer := NewTestConsumer()
	queue.PrepareConsumption(10)
	queue.AddConsumer("test", consumer)
	c.Check(consumer.LastDelivery, IsNil)

	c.Check(queue.Publish("1"), IsNil)
	time.Sleep(time.Millisecond)
	c.Assert(consumer.LastDelivery, NotNil)
	c.Check(consumer.LastDelivery.Payload(), Equals, "1")
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 1)

	c.Check(queue.Publish("2"), IsNil)
	time.Sleep(time.Millisecond)
	c.Check(consumer.LastDelivery.Payload(), Equals, "2")
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 2)

	c.Check(consumer.LastDeliveries[0].Ack(), Equals, true)
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 1)

	c.Check(consumer.LastDeliveries[1].Ack(), Equals, true)
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 0)

	c.Check(consumer.LastDeliveries[0].Ack(), Equals, false)
}

func (suite *QueueSuite) BenchmarkQueue(c *C) {
	// open queue
	host, port, db := suite.goenv.GetRedis()
	connection := OpenConnection("test", host, port, db)
	queueName := fmt.Sprintf("things-%d", c.N)
	queue := connection.OpenQueue(queueName)

	// add some consumers
	numConsumers := 10
	var consumers []*TestConsumer
	for i := 0; i < numConsumers; i++ {
		consumer := NewTestConsumer()
		consumer.AutoAck = true
		consumers = append(consumers, consumer)
		queue.PrepareConsumption(10)
		queue.AddConsumer("test", consumer)
	}

	// publish deliveries
	for i := 0; i < c.N; i++ {
		queue.Publish("foo")
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
}
