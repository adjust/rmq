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
	c.Check(delivery.Ack(), IsNil)
	c.Check(delivery.Payload(), Equals, "p23")
}

func (suite *DeliverySuite) TestDeliveryAck(c *C) {
	delivery := NewTestDelivery("p")
	c.Check(delivery.State, Equals, Unacked)
	c.Check(delivery.Ack(), IsNil)
	c.Check(delivery.State, Equals, Acked)

	c.Check(delivery.Ack(), Equals, ErrorNotFound)
	c.Check(delivery.Reject(), Equals, ErrorNotFound)
	c.Check(delivery.State, Equals, Acked)
}

func (suite *DeliverySuite) TestDeliveryReject(c *C) {
	delivery := NewTestDelivery("p")
	c.Check(delivery.State, Equals, Unacked)
	c.Check(delivery.Reject(), IsNil)
	c.Check(delivery.State, Equals, Rejected)

	c.Check(delivery.Reject(), Equals, ErrorNotFound)
	c.Check(delivery.Ack(), Equals, ErrorNotFound)
	c.Check(delivery.State, Equals, Rejected)
}
