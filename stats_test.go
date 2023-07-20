package rmq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStats(t *testing.T) {
	redisOptions, closer := testRedis(t)
	defer closer()

	connection, err := OpenConnectionWithRedisOptions("stats-conn", redisOptions, nil)
	assert.NoError(t, err)
	_, err = NewCleaner(connection).Clean()
	require.NoError(t, err)

	conn1, err := OpenConnectionWithRedisOptions("stats-conn1", redisOptions, nil)
	assert.NoError(t, err)
	conn2, err := OpenConnectionWithRedisOptions("stats-conn2", redisOptions, nil)
	assert.NoError(t, err)
	q1, err := conn2.OpenQueue("stats-q1")
	assert.NoError(t, err)
	_, err = q1.PurgeReady()
	assert.NoError(t, err)
	assert.NoError(t, q1.Publish("stats-d1"))
	q2, err := conn2.OpenQueue("stats-q2")
	assert.NoError(t, err)
	_, err = q2.PurgeReady()
	assert.NoError(t, err)
	consumer := NewTestConsumer("hand-A")
	consumer.AutoAck = false
	assert.NoError(t, q2.StartConsuming(10, time.Millisecond))
	_, err = q2.AddConsumer("stats-cons1", consumer)
	assert.NoError(t, err)
	assert.NoError(t, q2.Publish("stats-d2"))
	assert.NoError(t, q2.Publish("stats-d3"))
	assert.NoError(t, q2.Publish("stats-d4"))
	time.Sleep(2 * time.Millisecond)
	assert.NoError(t, consumer.Deliveries()[0].Ack())
	assert.NoError(t, consumer.Deliveries()[1].Reject())
	_, err = q2.AddConsumer("stats-cons2", NewTestConsumer("hand-B"))
	assert.NoError(t, err)

	queues, err := connection.GetOpenQueues()
	assert.NoError(t, err)
	stats, err := CollectStats(queues, connection)
	assert.NoError(t, err)
	// log.Printf("stats\n%s", stats)
	html := stats.GetHtml("", "")
	assert.Regexp(t, ".*queue.*ready.*connection.*unacked.*consumers.*q1.*1.*0.*0.*", html)
	assert.Regexp(t, ".*queue.*ready.*connection.*unacked.*consumers.*q2.*0.*1.*1.*2.*conn2.*1.*2.*", html)

	stats, err = CollectStats([]string{"stats-q1", "stats-q2"}, connection)
	assert.NoError(t, err)

	for key, _ := range stats.QueueStats {
		assert.Regexp(t, "stats.*", key)
	}
	/*
		<html><body><table style="font-family:monospace">
		<tr><td>queue</td><td></td><td>ready</td><td></td><td>rejected</td><td></td><td style="color:lightgrey">connection</td><td></td><td>unacked</td><td></td><td>consumers</td><td></td></tr>
		<tr><td>stats-q2</td><td></td><td>0</td><td></td><td>1</td><td></td><td></td><td></td><td>1</td><td></td><td>2</td><td></td></tr>
		<tr style="color:lightgrey"><td></td><td></td><td></td><td></td><td></td><td></td><td>stats-conn2-vY5ZPz</td><td></td><td>1</td><td></td><td>2</td><td></td></tr>
		<tr><td>stats-q1</td><td></td><td>1</td><td></td><td>0</td><td></td><td></td><td></td><td>0</td><td></td><td>0</td><td></td></tr>
		<tr><td>q2</td><td></td><td>0</td><td></td><td>0</td><td></td><td></td><td></td><td>0</td><td></td><td>0</td><td></td></tr>
		<tr><td>q1</td><td></td><td>0</td><td></td><td>0</td><td></td><td></td><td></td><td>0</td><td></td><td>0</td><td></td></tr>
		</table></body></html>
	*/

	q2.StopConsuming()
	assert.NoError(t, connection.stopHeartbeat())
	assert.NoError(t, conn1.stopHeartbeat())
	assert.NoError(t, conn2.stopHeartbeat())
}
