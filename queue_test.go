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

	queue := connection.OpenQueue("things")
	c.Assert(queue, NotNil)
	c.Check(connection.GetOpenQueues(), DeepEquals, []string{"things"})

	queue = connection.OpenQueue("balls")
	c.Assert(queue, NotNil)
	c.Check(connection.GetOpenQueues(), DeepEquals, []string{"balls", "things"})

	c.Check(connection.CloseQueue("apples"), Equals, false)

	c.Check(connection.CloseQueue("balls"), Equals, true)
	c.Check(connection.GetOpenQueues(), DeepEquals, []string{"things"})

	c.Check(connection.CloseQueue("things"), Equals, true)
	c.Check(connection.GetOpenQueues(), DeepEquals, []string{})
}
