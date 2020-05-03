package rmq

import (
	"testing"

	. "github.com/adjust/gocheck"
)

func TestConnectionSuite(t *testing.T) {
	TestingSuiteT(&ConnectionSuite{}, t)
}

type ConnectionSuite struct{}

func (suite *ConnectionSuite) TestConnection(c *C) {
	connection := NewTestConnection()
	var _ Connection = connection // check that it implements the interface
	c.Check(connection.GetDelivery("things", 0), Equals, "rmq.TestConnection: delivery not found: things[0]")

	queue, err := connection.OpenQueue("things")
	c.Check(err, IsNil)
	c.Check(connection.GetDelivery("things", -1), Equals, "rmq.TestConnection: delivery not found: things[-1]")
	c.Check(connection.GetDelivery("things", 0), Equals, "rmq.TestConnection: delivery not found: things[0]")
	c.Check(connection.GetDelivery("things", 1), Equals, "rmq.TestConnection: delivery not found: things[1]")

	c.Check(queue.Publish("bar"), IsNil)
	c.Check(connection.GetDelivery("things", 0), Equals, "bar")
	c.Check(connection.GetDelivery("things", 1), Equals, "rmq.TestConnection: delivery not found: things[1]")
	c.Check(connection.GetDelivery("things", 2), Equals, "rmq.TestConnection: delivery not found: things[2]")

	c.Check(queue.Publish("foo"), IsNil)
	c.Check(connection.GetDelivery("things", 0), Equals, "bar")
	c.Check(connection.GetDelivery("things", 1), Equals, "foo")
	c.Check(connection.GetDelivery("things", 2), Equals, "rmq.TestConnection: delivery not found: things[2]")

	connection.Reset()
	c.Check(connection.GetDelivery("things", 0), Equals, "rmq.TestConnection: delivery not found: things[0]")

	c.Check(queue.Publish("blab"), IsNil)
	c.Check(connection.GetDelivery("things", 0), Equals, "blab")
	c.Check(connection.GetDelivery("things", 1), Equals, "rmq.TestConnection: delivery not found: things[1]")
}
