package rmq

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func testRedis(t testing.TB) (options *redis.Options, close func()) {
	t.Helper()

	if redisAddr, ok := os.LookupEnv("REDIS_ADDR"); ok {
		return &redis.Options{Addr: redisAddr}, func() {}
	}

	mr := miniredis.RunT(t)
	return &redis.Options{Addr: mr.Addr()}, mr.Close
}

func testClusterRedis(t testing.TB) (options *redis.ClusterOptions, close func()) {
	t.Helper()

	// Follow these steps to set up a local redis cluster:
	// https://github.com/redis/redis/tree/unstable/utils/create-cluster
	// Then run the tests like this:
	// REDIS_CLUSTER_ADDR="localhost:30001,localhost:30002,localhost:30003,localhost:30004,localhost:30005,localhost:30006" go test
	if redisAddrs, ok := os.LookupEnv("REDIS_CLUSTER_ADDR"); ok {
		addrs := strings.Split(redisAddrs, ",")
		return &redis.ClusterOptions{Addrs: addrs}, func() {}
	}

	mr1 := miniredis.RunT(t)
	mr2 := miniredis.RunT(t)
	mr3 := miniredis.RunT(t)

	options = &redis.ClusterOptions{
		Addrs: []string{
			mr1.Addr(),
			mr2.Addr(),
			mr3.Addr(),
		},
	}

	closeFunc := func() {
		mr1.Close()
		mr2.Close()
		mr3.Close()
	}

	return options, closeFunc
}

func eventuallyReady(t *testing.T, queue Queue, expectedReady int64) {
	t.Helper()
	assert.Eventually(t, func() bool {
		count, err := queue.readyCount()
		if err != nil {
			return false
		}
		return count == expectedReady
	}, 1*time.Second, 2*time.Millisecond)
}

func eventuallyUnacked(t *testing.T, queue Queue, expectedUnacked int64) {
	t.Helper()
	assert.Eventually(t, func() bool {
		count, err := queue.unackedCount()
		if err != nil {
			return false
		}
		return count == expectedUnacked
	}, 1*time.Second, 2*time.Millisecond)
}

func eventuallyRejected(t *testing.T, queue Queue, expectedRejected int64) {
	t.Helper()
	assert.Eventually(t, func() bool {
		count, err := queue.rejectedCount()
		if err != nil {
			return false
		}
		return count == expectedRejected
	}, 1*time.Second, 2*time.Millisecond)
}
