package rmq

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func eventuallyReady(t *testing.T, queue Queue, expectedReady int64) {
	t.Helper()
	assert.Eventually(t, func() bool {
		count, err := queue.readyCount()
		if err != nil {
			return false
		}
		return count == expectedReady
	}, 10*time.Second, 2*time.Millisecond)
}

func eventuallyUnacked(t *testing.T, queue Queue, expectedUnacked int64) {
	t.Helper()
	assert.Eventually(t, func() bool {
		count, err := queue.unackedCount()
		if err != nil {
			return false
		}
		return count == expectedUnacked
	}, 10*time.Second, 2*time.Millisecond)
}

func eventuallyRejected(t *testing.T, queue Queue, expectedRejected int64) {
	t.Helper()
	assert.Eventually(t, func() bool {
		count, err := queue.rejectedCount()
		if err != nil {
			return false
		}
		return count == expectedRejected
	}, 10*time.Second, 2*time.Millisecond)
}
