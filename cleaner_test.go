package rmq

import (
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testRedis(t testing.TB) (addr string, closer func()) {
	t.Helper()

	if redisAddr, ok := os.LookupEnv("REDIS_ADDR"); ok {
		return redisAddr, func() {}
	}

	mr := miniredis.RunT(t)
	return mr.Addr(), mr.Close
}

func publishDeliveries(t *testing.T, queue Queue, ready, unAcked int64, deliveries ...string) (int64, int64) {
	t.Helper()
	checkReadyAndUnAcked(t, queue, ready, unAcked)
	for _, delivery := range deliveries {
		assert.NoError(t, queue.Publish(delivery))
		ready += 1
		checkReadyAndUnAcked(t, queue, ready, unAcked)
	}
	return ready, unAcked
}

// consume cnt deliveries into chan buffer
func preConsume(t *testing.T, queue Queue, ready, unAcked, cnt int64) (int64, int64) {
	t.Helper()
	eventuallyUnacked(t, queue, unAcked)
	assert.NoError(t, queue.StartConsuming(cnt, time.Millisecond))
	ready -= cnt
	unAcked += cnt
	checkReadyAndUnAcked(t, queue, ready, unAcked)
	return ready, unAcked
}

// ack finished deliveries
func consumerAck(t *testing.T, queue Queue, consumer *TestConsumer, ready, unAcked int64, expected ...string) (int64, int64) {
	t.Helper()
	for _, exp := range expected {
		require.NotNil(t, consumer.Last())
		checkLast(t, queue, consumer, ready, unAcked, exp)
		assert.NoError(t, consumer.Last().Ack())
		ready -= 1
		checkReadyAndUnAcked(t, queue, ready, unAcked)
	}
	return ready, unAcked
}

// receive deliveries from chan buffer and finish them
func consumeWithoutAck(t *testing.T, queue Queue, consumer *TestConsumer, ready, unAcked int64, expected ...string) {
	t.Helper()
	for _, exp := range expected {
		consumer.Finish() // unacked
		time.Sleep(10 * time.Millisecond)
		checkLast(t, queue, consumer, ready, unAcked, exp)
	}
}

// check last received deivery
func checkLast(t *testing.T, queue Queue, consumer *TestConsumer, ready, unAcked int64, expected string) {
	t.Helper()
	checkReadyAndUnAcked(t, queue, ready, unAcked)
	assert.Equal(t, expected, consumer.Last().Payload())
}

func checkReadyAndUnAcked(t *testing.T, queue Queue, ready, unAcked int64) {
	t.Helper()
	eventuallyUnacked(t, queue, unAcked)
	eventuallyReady(t, queue, ready)
}

func genDeliveries(start, end int) []string {
	res := make([]string, end-start+1)
	for i := 0; i <= end-start; i++ {
		res[i] = "del" + strconv.Itoa(i+start)
	}
	return res
}

func assertQueueNum(t *testing.T, conn Connection, num int) {
	t.Helper()
	queues, err := conn.GetOpenQueues()
	assert.NoError(t, err)
	assert.Len(t, queues, num)
}

func newManualConsumer(name string) *TestConsumer {
	consumer := NewTestConsumer(name)
	consumer.AutoFinish = false
	consumer.AutoAck = false
	return consumer
}

func queueAddManualConsume(t *testing.T, queue Queue, name string) *TestConsumer {
	consumer := newManualConsumer(name)
	_, err := queue.AddConsumer(name, consumer) // take one 'del' from chan buffer and wait finish
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
	return consumer
}

func TestCleaner(t *testing.T) {
	redisAddr, closer := testRedis(t)
	defer closer()

	flushConn, err := OpenConnection("cleaner-flush", "tcp", redisAddr, 1, nil)
	assert.NoError(t, err)
	assert.NoError(t, flushConn.stopHeartbeat())
	assert.NoError(t, flushConn.flushDb())

	conn, err := OpenConnection("cleaner-conn1", "tcp", redisAddr, 1, nil)
	assert.NoError(t, err)
	assertQueueNum(t, conn, 0)

	queue1, err := conn.OpenQueue("q1")
	assert.NoError(t, err)
	assertQueueNum(t, conn, 1)

	queue2, err := conn.OpenQueue("q2")
	assert.NoError(t, err)
	assertQueueNum(t, conn, 2)

	var (
		ready1    = int64(0)
		unAcked1A = int64(0)
		ready2    = int64(0)
		unAcked2A = int64(0)
	)

	ready1, unAcked1A = publishDeliveries(t, queue1, ready1, unAcked1A, genDeliveries(1, 6)...) // pub 1...6
	ready1, unAcked1A = preConsume(t, queue1, ready1, unAcked1A, 2)                             // take 1, 2 into chan buffer
	ready2, unAcked2A = publishDeliveries(t, queue2, ready2, unAcked2A, genDeliveries(1, 9)...) // pub 1...9
	ready2, unAcked2A = preConsume(t, queue2, ready2, unAcked2A, 5)                             // take 1...5 into chan buffer

	consumer1A := queueAddManualConsume(t, queue1, "C-1-A") // take 1 from chan buffer and wait finish
	consumer2A := queueAddManualConsume(t, queue2, "C-2-A") // take 1 from chan buffer and wait finish

	ready1, unAcked1A = consumerAck(t, queue1, consumer1A, ready1, unAcked1A, genDeliveries(1, 1)...) // ack 1 -> take 3
	ready2, unAcked2A = consumerAck(t, queue2, consumer2A, ready2, unAcked2A, genDeliveries(1, 1)...) // ack 1 -> take 6
	time.Sleep(10 * time.Millisecond)
	consumer1A.FinishAll()
	consumer2A.FinishAll()

	<-conn.StopAllConsuming()
	time.Sleep(time.Millisecond)

	conn, err = OpenConnection("cleaner-conn1", "tcp", redisAddr, 1, nil)
	assert.NoError(t, err)
	unAcked1B := int64(0)
	unAcked2B := int64(0)
	queue1, err = conn.OpenQueue("q1")
	assert.NoError(t, err)
	queue2, err = conn.OpenQueue("q2")
	assert.NoError(t, err)

	ready1, unAcked1B = publishDeliveries(t, queue1, ready1, unAcked1B, genDeliveries(7, 11)...) // pub 7...11

	ready1, unAcked1B = preConsume(t, queue1, ready1, unAcked1B, 2) // take 4, 5 into chan buffer
	ready2, unAcked2B = preConsume(t, queue2, ready2, unAcked2B, 2) // take 7, 8 into chan buffer

	consumer1B := queueAddManualConsume(t, queue1, "C-1-B") // take 4 from chan buffer and wait finish

	consumeWithoutAck(t, queue1, consumer1B, ready1, unAcked1B, genDeliveries(5, 5)...) // finish 4 and take 5 from chan buffer
	time.Sleep(10 * time.Millisecond)
	ready1, unAcked1B = consumerAck(t, queue1, consumer1B, ready1, unAcked1B, genDeliveries(5, 5)...) // ack 5 -> take 6

	consumer1B.FinishAll()
	<-conn.StopAllConsuming()
	time.Sleep(time.Millisecond)

	cleanerConn, err := OpenConnection("cleaner-conn", "tcp", redisAddr, 1, nil)
	assert.NoError(t, err)
	unAcked1C := int64(0)
	unAcked2C := int64(0)
	cleaner := NewCleaner(cleanerConn)
	returned, err := cleaner.Clean()
	assert.NoError(t, err)
	// queue1: 7..11 in ready. 2,3,4,6 in unacked. 1, 5 acked.
	// queue2: 9 in ready, 2...8 in unacked, 1 acked.
	assert.Equal(t, unAcked1A+unAcked1B+unAcked2A+unAcked2B, returned)
	ready1 += unAcked1A + unAcked1B
	ready2 += unAcked2A + unAcked2B

	checkReadyAndUnAcked(t, queue1, ready1, unAcked1C)
	checkReadyAndUnAcked(t, queue2, ready2, unAcked2C)
	assertQueueNum(t, conn, 2)

	conn, err = OpenConnection("cleaner-conn1", "tcp", redisAddr, 1, nil)
	assert.NoError(t, err)
	queue1, err = conn.OpenQueue("q1")
	assert.NoError(t, err)
	assert.NoError(t, queue1.StartConsuming(ready1+1, time.Millisecond))
	consumerC := NewTestConsumer("c-C")

	_, err = queue1.AddConsumer("consumer3", consumerC)
	assert.NoError(t, err)
	time.Sleep(10 * time.Millisecond)
	assert.Eventually(t, func() bool {
		return len(consumerC.Deliveries()) == int(ready1)
	}, 10*time.Second, 2*time.Millisecond)

	<-conn.StopAllConsuming()
	time.Sleep(time.Millisecond)

	returned, err = cleaner.Clean()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), returned)
	assert.NoError(t, cleanerConn.stopHeartbeat())
}
