package rmq

import (
	"errors"
	"fmt"
)

var (
	ErrorNotFound         = errors.New("entity not found") // entitify being connection/queue/delivery
	ErrorAlreadyConsuming = errors.New("must not call StartConsuming() multiple times")
	ErrorNotConsuming     = errors.New("must call StartConsuming() before adding consumers")
	ErrorConsumingStopped = errors.New("consuming stopped")
)

type ConsumeError struct {
	RedisErr error
	Count    int // number of consecutive errors
}

func (e *ConsumeError) Error() string {
	return fmt.Sprintf("rmq.ConsumeError (%d): %s", e.Count, e.RedisErr.Error())
}

func (e *ConsumeError) Unwrap() error {
	return e.RedisErr
}

type HeartbeatError struct {
	RedisErr error
	Count    int // number of consecutive errors
}

func (e *HeartbeatError) Error() string {
	return fmt.Sprintf("rmq.HeartbeatError (%d): %s", e.Count, e.RedisErr.Error())
}

func (e *HeartbeatError) Unwrap() error {
	return e.RedisErr
}

type DeliveryError struct {
	Delivery Delivery
	RedisErr error
	Count    int // number of consecutive errors
}

func (e *DeliveryError) Error() string {
	return fmt.Sprintf("rmq.DeliveryError (%d): %s", e.Count, e.RedisErr.Error())
}

func (e *DeliveryError) Unwrap() error {
	return e.RedisErr
}
