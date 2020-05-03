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
	err := delivery.Ack()
	c.Check(err, IsNil)
	c.Check(delivery.Payload(), Equals, "p23")
}

func (suite *DeliverySuite) TestDeliveryAck(c *C) {
	delivery := NewTestDelivery("p")
	c.Check(delivery.State, Equals, Unacked)
	err := delivery.Ack()
	c.Check(err, IsNil)
	c.Check(delivery.State, Equals, Acked)

	err = delivery.Ack()
	c.Check(err, Equals, ErrorNotFound)
	err = delivery.Reject()
	c.Check(err, Equals, ErrorNotFound)
	c.Check(delivery.State, Equals, Acked)
}

func (suite *DeliverySuite) TestDeliveryReject(c *C) {
	delivery := NewTestDelivery("p")
	c.Check(delivery.State, Equals, Unacked)
	err := delivery.Reject()
	c.Check(err, IsNil)
	c.Check(delivery.State, Equals, Rejected)

	err = delivery.Reject()
	c.Check(err, Equals, ErrorNotFound)
	err = delivery.Ack()
	c.Check(err, Equals, ErrorNotFound)
	c.Check(delivery.State, Equals, Rejected)
}
