package rmq

import (
	"testing"

	. "github.com/adjust/gocheck"
)

func TestDeliverySuite(t *testing.T) {
	TestingSuiteT(&DeliverySuite{}, t)
}

type DeliverySuite struct {
}

func (suite *DeliverySuite) TestDeliveryPayload(c *C) {
	var delivery Delivery
	delivery = NewTestDelivery("p23")
	ok, err := delivery.Ack()
	c.Check(err, IsNil)
	c.Check(ok, Equals, true)
	c.Check(delivery.Payload(), Equals, "p23")
}

func (suite *DeliverySuite) TestDeliveryAck(c *C) {
	delivery := NewTestDelivery("p")
	c.Check(delivery.State, Equals, Unacked)
	ok, err := delivery.Ack()
	c.Check(err, IsNil)
	c.Check(ok, Equals, true)
	c.Check(delivery.State, Equals, Acked)

	ok, err = delivery.Ack()
	c.Check(err, IsNil)
	c.Check(ok, Equals, false)
	ok, err = delivery.Reject()
	c.Check(err, IsNil)
	c.Check(ok, Equals, false)
	c.Check(delivery.State, Equals, Acked)
}

func (suite *DeliverySuite) TestDeliveryReject(c *C) {
	delivery := NewTestDelivery("p")
	c.Check(delivery.State, Equals, Unacked)
	ok, err := delivery.Reject()
	c.Check(err, IsNil)
	c.Check(ok, Equals, true)
	c.Check(delivery.State, Equals, Rejected)

	ok, err = delivery.Reject()
	c.Check(err, IsNil)
	c.Check(ok, Equals, false)
	ok, err = delivery.Ack()
	c.Check(err, IsNil)
	c.Check(ok, Equals, false)
	c.Check(delivery.State, Equals, Rejected)
}
