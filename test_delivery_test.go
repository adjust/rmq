package rmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeliveryPayload(t *testing.T) {
	var delivery Delivery
	delivery = NewTestDelivery("p23")
	assert.NoError(t, delivery.Ack())
	assert.Equal(t, "p23", delivery.Payload())
}

func TestDeliveryAck(t *testing.T) {
	delivery := NewTestDelivery("p")
	assert.Equal(t, Unacked, delivery.State)
	assert.NoError(t, delivery.Ack())
	assert.Equal(t, Acked, delivery.State)

	assert.Equal(t, ErrorNotFound, delivery.Ack())
	assert.Equal(t, ErrorNotFound, delivery.Reject())
	assert.Equal(t, Acked, delivery.State)
}

func TestDeliveryReject(t *testing.T) {
	delivery := NewTestDelivery("p")
	assert.Equal(t, Unacked, delivery.State)
	assert.NoError(t, delivery.Reject())
	assert.Equal(t, Rejected, delivery.State)

	assert.Equal(t, ErrorNotFound, delivery.Reject())
	assert.Equal(t, ErrorNotFound, delivery.Ack())
	assert.Equal(t, Rejected, delivery.State)
}
