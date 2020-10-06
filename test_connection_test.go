package rmq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTestConnection(t *testing.T) {
	connection := NewTestConnection()
	var _ Connection = connection // check that it implements the interface
	assert.Equal(t, "rmq.TestConnection: delivery not found: things[0]", connection.GetDelivery("things", 0))

	queue, err := connection.OpenQueue("things")
	assert.NoError(t, err)
	assert.Equal(t, "rmq.TestConnection: delivery not found: things[-1]", connection.GetDelivery("things", -1))
	assert.Equal(t, "rmq.TestConnection: delivery not found: things[0]", connection.GetDelivery("things", 0))
	assert.Equal(t, "rmq.TestConnection: delivery not found: things[1]", connection.GetDelivery("things", 1))

	assert.NoError(t, queue.Publish("bar"))
	assert.Equal(t, "bar", connection.GetDelivery("things", 0))
	assert.Equal(t, "rmq.TestConnection: delivery not found: things[1]", connection.GetDelivery("things", 1))
	assert.Equal(t, "rmq.TestConnection: delivery not found: things[2]", connection.GetDelivery("things", 2))

	assert.NoError(t, queue.Publish("foo"))
	assert.Equal(t, "bar", connection.GetDelivery("things", 0))
	assert.Equal(t, "foo", connection.GetDelivery("things", 1))
	assert.Equal(t, "rmq.TestConnection: delivery not found: things[2]", connection.GetDelivery("things", 2))

	connection.Reset()
	assert.Equal(t, "rmq.TestConnection: delivery not found: things[0]", connection.GetDelivery("things", 0))

	assert.NoError(t, queue.Publish("blab"))
	assert.Equal(t, "blab", connection.GetDelivery("things", 0))
	assert.Equal(t, "rmq.TestConnection: delivery not found: things[1]", connection.GetDelivery("things", 1))
}
