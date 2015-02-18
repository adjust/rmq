package queue

import (
	"log"
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

func (suite *HandlerSuite) TestConnections(c *C) {
	log.Printf("\n\n")

	host, port, db := suite.goenv.GetRedis()
	connection := OpenConnection("test", host, port, db)
	connection.CloseAllConnections()
	connection.CloseAllQueues()

	OpenConnection("conn1", host, port, db)
	conn2 := OpenConnection("conn2", host, port, db)
	conn2.OpenQueue("q1")
	q2 := conn2.OpenQueue("q2")
	consumer := NewTestConsumer()
	q2.AddConsumer("cons1", consumer)
	q2.Publish("d1")
	q2.Publish("d2")
	consumer.LastDelivery.Ack()
	q2.AddConsumer("cons2", NewTestConsumer())

	time.Sleep(100 * time.Millisecond)

	handler := NewHandler(connection)
	request, err := http.NewRequest("GET", "https://app.adjust.com/redis_queue", nil)
	c.Assert(err, IsNil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
}
