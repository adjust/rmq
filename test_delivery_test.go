package rmq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDeliveryPayload(t *testing.T) {
	var delivery Delivery
	delivery = NewTestDelivery("p23")
	assert.NoError(t, delivery.Ack(context.Background()))
	assert.Equal(t, "p23", delivery.Payload())
}

func TestDeliveryAck(t *testing.T) {
	ctx := context.Background()
	delivery := NewTestDelivery("p")
	assert.Equal(t, Unacked, delivery.State)
	assert.NoError(t, delivery.Ack(ctx))
	assert.Equal(t, Acked, delivery.State)

	assert.Equal(t, ErrorNotFound, delivery.Ack(ctx))
	assert.Equal(t, ErrorNotFound, delivery.Reject(ctx))
	assert.Equal(t, Acked, delivery.State)
}

func TestDeliveryReject(t *testing.T) {
	ctx := context.Background()
	delivery := NewTestDelivery("p")
	assert.Equal(t, Unacked, delivery.State)
	assert.NoError(t, delivery.Reject(ctx))
	assert.Equal(t, Rejected, delivery.State)

	assert.Equal(t, ErrorNotFound, delivery.Reject(ctx))
	assert.Equal(t, ErrorNotFound, delivery.Ack(ctx))
	assert.Equal(t, Rejected, delivery.State)
}
