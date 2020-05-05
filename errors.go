package rmq

type ConsumeError struct {
	RedisErr error
}

func (e *ConsumeError) Error() string {
	return "rmq.ConsumeError: " + e.RedisErr.Error()
}
