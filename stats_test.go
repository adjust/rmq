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
	connection, err := OpenConnection("stats-conn", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	_, err = NewCleaner(connection).Clean()
	c.Assert(err, IsNil)

	conn1, err := OpenConnection("stats-conn1", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	conn2, err := OpenConnection("stats-conn2", "tcp", "localhost:6379", 1)
	c.Check(err, IsNil)
	q1, err := conn2.OpenQueue("stats-q1")
	c.Check(err, IsNil)
	_, err = q1.PurgeReady()
	c.Check(err, IsNil)
	c.Check(q1.Publish("stats-d1"), IsNil)
	q2, err := conn2.OpenQueue("stats-q2")
	c.Check(err, IsNil)
	_, err = q2.PurgeReady()
	c.Check(err, IsNil)
	consumer := NewTestConsumer("hand-A")
	consumer.AutoAck = false
	c.Check(q2.StartConsuming(10, time.Millisecond, nil), IsNil)
	_, err = q2.AddConsumer("stats-cons1", consumer)
	c.Check(err, IsNil)
	c.Check(q2.Publish("stats-d2"), IsNil)
	c.Check(q2.Publish("stats-d3"), IsNil)
	c.Check(q2.Publish("stats-d4"), IsNil)
	time.Sleep(2 * time.Millisecond)
	c.Check(consumer.LastDeliveries[0].Ack(), IsNil)
	c.Check(consumer.LastDeliveries[1].Reject(), IsNil)
	_, err = q2.AddConsumer("stats-cons2", NewTestConsumer("hand-B"))
	c.Check(err, IsNil)

	queues, err := connection.GetOpenQueues()
	c.Check(err, IsNil)
	stats, err := CollectStats(queues, connection)
	c.Check(err, IsNil)
	// log.Printf("stats\n%s", stats)
	html := stats.GetHtml("", "")
	c.Check(html, Matches, ".*queue.*ready.*connection.*unacked.*consumers.*q1.*1.*0.*0.*")
	c.Check(html, Matches, ".*queue.*ready.*connection.*unacked.*consumers.*q2.*0.*1.*1.*2.*conn2.*1.*2.*")

	stats, err = CollectStats([]string{"stats-q1", "stats-q2"}, connection)
	c.Check(err, IsNil)

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
	c.Check(connection.stopHeartbeat(), IsNil)
	c.Check(conn1.stopHeartbeat(), IsNil)
	c.Check(conn2.stopHeartbeat(), IsNil)
}
