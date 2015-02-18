package queue

import (
	"testing"

	"github.com/adjust/goenv"

	. "github.com/adjust/gocheck"
)

func TestCleanerSuite(t *testing.T) {
	TestingSuiteT(&CleanerSuite{}, t)
}

type CleanerSuite struct {
	goenv *goenv.Goenv
}

func (suite *CleanerSuite) SetUpSuite(c *C) {
	suite.goenv = goenv.TestGoenv()
}

func (suite *CleanerSuite) TestCleaner(c *C) {
	host, port, db := suite.goenv.GetRedis()
	connection := OpenConnection("cleaner", host, port, db)

	conn1 := OpenConnection("conn1", host, port, db)
	conn1.OpenQueue("queue1")
	conn1.StopHeartbeat()

	cleaner := NewCleaner(connection)
	cleaner.Clean()
}
