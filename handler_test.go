package queue

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/adjust/goenv"

	. "github.com/adjust/gocheck"
)

func TestHandlerSuite(t *testing.T) {
	TestingSuiteT(&HandlerSuite{}, t)
}

type HandlerSuite struct {
	goenv *goenv.Goenv
}

func (suite *HandlerSuite) SetUpSuite(c *C) {
	suite.goenv = goenv.TestGoenv()
}

func (suite *HandlerSuite) TestHandler(c *C) {
	host, port, db := suite.goenv.GetRedis()
	connection := OpenConnection("handler-conn", host, port, db)
	c.Assert(NewCleaner(connection).Clean(), IsNil)

	conn1 := OpenConnection("handler-conn1", host, port, db)
	conn2 := OpenConnection("handler-conn2", host, port, db)
	q1 := conn2.OpenQueue("handler-q1")
	q1.Purge()
	q1.Publish("handler-d1")
	q2 := conn2.OpenQueue("handler-q2")
	q2.Purge()
	consumer := NewTestConsumer()
	q2.StartConsuming(10)
	q2.AddConsumer("handler-cons1", consumer)
	q2.Publish("handler-d2")
	q2.Publish("handler-d3")
	time.Sleep(2 * time.Millisecond)
	consumer.LastDelivery.Ack()
	q2.AddConsumer("handler-cons2", NewTestConsumer())

	time.Sleep(5 * time.Millisecond)

	handler := NewHandler(connection)
	request, err := http.NewRequest("GET", "https://app.adjust.com/redis_queue", nil)
	c.Assert(err, IsNil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	c.Check(recorder.Body.String(), Matches, ".*queue.*ready.*connection.*unacked.*consumers.*q1.*1.*0.*0.*")
	c.Check(recorder.Body.String(), Matches, ".*queue.*ready.*connection.*unacked.*consumers.*q2.*0.*1.*2.*conn2.*1.*2.*")
	/*
		<html><body><table style="font-family:monospace">
		<tr><td>queue</td><td></td><td>ready</td><td></td><td style="color:lightgrey">connection</td><td></td><td>unacked</td><td></td><td>consumers</td><td></td></tr>
		<tr><td>handler-q2</td><td></td><td>0</td><td></td><td></td><td></td><td>0</td><td></td><td>2</td><td></td></tr>
		<tr style="color:lightgrey"><td></td><td></td><td></td><td></td><td>handler-conn2-x7M8CP</td><td></td><td>0</td><td></td><td>2</td><td></td></tr>
		<tr><td>handler-q1</td><td></td><td>1</td><td></td><td></td><td></td><td>0</td><td></td><td>0</td><td></td></tr>
		<tr><td>cleaner-queue1</td><td></td><td>0</td><td></td><td></td><td></td><td>0</td><td></td><td>0</td><td></td></tr>
		</table></body></html>
	*/

	q2.StopConsuming()
	connection.StopHeartbeat()
	conn1.StopHeartbeat()
	conn2.StopHeartbeat()
}
