package rmq

import (
	"testing"
	"time"

	. "github.com/adjust/gocheck"
)

func TestStatsSuite(t *testing.T) {
	TestingSuiteT(&StatsSuite{}, t)
}

type StatsSuite struct{}

func (suite *StatsSuite) TestStats(c *C) {
	connection := OpenConnection("stats-conn", "tcp", "localhost:6379", 1)
	c.Assert(NewCleaner(connection).Clean(), IsNil)

	conn1 := OpenConnection("stats-conn1", "tcp", "localhost:6379", 1)
	conn2 := OpenConnection("stats-conn2", "tcp", "localhost:6379", 1)
	q1 := conn2.OpenQueue("stats-q1").(*redisQueue)
	q1.PurgeReady()
	q1.Publish("stats-d1")
	q2 := conn2.OpenQueue("stats-q2").(*redisQueue)
	q2.PurgeReady()
	consumer := NewTestConsumer("hand-A")
	consumer.AutoAck = false
	q2.StartConsuming(10, time.Millisecond)
	q2.AddConsumer("stats-cons1", consumer)
	q2.Publish("stats-d2")
	q2.Publish("stats-d3")
	q2.Publish("stats-d4")
	time.Sleep(2 * time.Millisecond)
	consumer.LastDeliveries[0].Ack()
	consumer.LastDeliveries[1].Reject()
	q2.AddConsumer("stats-cons2", NewTestConsumer("hand-B"))

	stats := CollectStats(connection.GetOpenQueues(), connection)
	// log.Printf("stats\n%s", stats)
	html := stats.GetHtml("", "")
	c.Check(html, Matches, ".*queue.*ready.*connection.*unacked.*consumers.*q1.*1.*0.*0.*")
	c.Check(html, Matches, ".*queue.*ready.*connection.*unacked.*consumers.*q2.*0.*1.*1.*2.*conn2.*1.*2.*")

	stats = CollectStats([]string{"stats-q1", "stats-q2"}, connection)

	for key, _ := range stats.QueueStats {
		c.Check(key, Matches, "stats.*")
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
	connection.StopHeartbeat()
	conn1.StopHeartbeat()
	conn2.StopHeartbeat()
}
