package rmq

type ConsumeError struct {
	RedisErr error
}

func (e *ConsumeError) Error() string {
	return "rmq.ConsumeError: " + e.RedisErr.Error()
}

type HeartbeatError struct {
	RedisErr error
}

func (e *HeartbeatError) Error() string {
	return "rmq.HeartbeatError: " + e.RedisErr.Error()
}
