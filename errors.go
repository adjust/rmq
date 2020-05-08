package rmq

import "fmt"

type ConsumeError struct {
	RedisErr error
	Count    int // number of consecutive errors
}

func (e *ConsumeError) Error() string {
	return fmt.Sprintf("rmq.ConsumeError (%d): %s", e.Count, e.RedisErr.Error())
}

type HeartbeatError struct {
	RedisErr error
	Count    int // number of consecutive errors
}

func (e *HeartbeatError) Error() string {
	return fmt.Sprintf("rmq.HeartbeatError (%d): %s", e.Count, e.RedisErr.Error())
}
