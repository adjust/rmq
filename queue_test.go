package queue

import (
	"testing"

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

func (suite *QueueSuite) TestConnection(c *C) {
	connection := NewConnection(suite.goenv.GetRedis())
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
	connection := NewConnection(suite.goenv.GetRedis())
	c.Assert(connection, NotNil)

	queue := connection.OpenQueue("things")
	queue.Clear()
	c.Check(queue.Length(), Equals, 0)
	c.Check(queue.Publish("test"), IsNil)
	c.Check(queue.Length(), Equals, 1)
	c.Check(queue.Publish("test"), IsNil)
	c.Check(queue.Length(), Equals, 2)

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
