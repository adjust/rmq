package queue

import (
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
	c.Check(connection.CheckConnection("nope"), Equals, false)
	c.Check(connection.CheckConnection(conn1.Name), Equals, true)
	conn2 := OpenConnection("test2", host, port, db)
	c.Check(connection.GetConnections(), HasLen, 2)
	c.Check(connection.CheckConnection(conn1.Name), Equals, true)
	c.Check(connection.CheckConnection(conn2.Name), Equals, true)

	c.Check(connection.CloseConnection("nope"), Equals, false)
	c.Check(connection.CloseConnection(conn1.Name), Equals, true)
	c.Check(connection.CloseConnection(conn1.Name), Equals, false)
	c.Check(connection.GetConnections(), DeepEquals, []string{conn2.Name})

	c.Check(connection.CloseConnection(conn2.Name), Equals, true)
	c.Check(connection.GetConnections(), HasLen, 0)
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

	queue.RemoveAllConsumers()
	c.Check(queue.GetConsumers(), HasLen, 0)
	nameTest := queue.AddConsumer("test", nil)
	c.Check(queue.GetConsumers(), DeepEquals, []string{nameTest})
	nameFoo := queue.AddConsumer("foo", nil)
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

	queue := connection.OpenQueue("things")
	c.Assert(queue, NotNil)
	queue.Clear()

	consumer := NewTestConsumer()
	queue.AddConsumer("test", consumer)
	c.Check(consumer.LastDelivery, IsNil)

	c.Check(queue.Publish("1"), IsNil)
	time.Sleep(time.Microsecond)
	c.Check(consumer.LastDelivery.Payload(), Equals, "1")
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 1)

	c.Check(queue.Publish("2"), IsNil)
	time.Sleep(time.Microsecond)
	c.Check(consumer.LastDelivery.Payload(), Equals, "2")
	c.Check(queue.ReadyCount(), Equals, 0)
	c.Check(queue.UnackedCount(), Equals, 2)
}
