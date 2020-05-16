package rmq

import (
	"context"
	"fmt"
	"time"
)

type Delivery interface {
	Payload() string

	Ack() error
	Reject() error
	Push() error

	AckWithRetry(context.Context, chan<- error) error
	RejectWithRetry(context.Context, chan<- error) error
	PushWithRetry(context.Context, chan<- error) error
}

// TODO: rename to redisDelivery
type wrapDelivery struct {
	payload     string
	unackedKey  string
	rejectedKey string
	pushKey     string
	redisClient RedisClient
}

func newDelivery(payload, unackedKey, rejectedKey, pushKey string, redisClient RedisClient) *wrapDelivery {
	return &wrapDelivery{
		payload:     payload,
		unackedKey:  unackedKey,
		rejectedKey: rejectedKey,
		pushKey:     pushKey,
		redisClient: redisClient,
	}
}

func (delivery *wrapDelivery) String() string {
	return fmt.Sprintf("[%s %s]", delivery.payload, delivery.unackedKey)
}

func (delivery *wrapDelivery) Payload() string {
	return delivery.payload
}

func (delivery *wrapDelivery) Ack() error {
	count, err := delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrorNotFound
	}
	return nil
}

func (delivery *wrapDelivery) Reject() error {
	return delivery.move(delivery.rejectedKey)
}

func (delivery *wrapDelivery) Push() error {
	if delivery.pushKey == "" {
		return delivery.Reject() // fall back to rejecting
	}

	return delivery.move(delivery.pushKey)
}

func (delivery *wrapDelivery) move(key string) error {
	if _, err := delivery.redisClient.LPush(key, delivery.payload); err != nil {
		return err
	}

	return delivery.Ack()
}

// blocking versions of the functions above with the following behavior:
// 1. return immediately if the operation succeeded or failed with ErrorNotFound
// 2. in case of other redis errors, send them to the errors chan and retry after a sleep
// 3. if the context is cancalled or its timeout exceeded, context.Cancelled or
//    context.DeadlineExceeded will be returned

func (delivery *wrapDelivery) AckWithRetry(ctx context.Context, errors chan<- error) error {
	return delivery.ackWithRetry(ctx, errors, 0)
}

func (delivery *wrapDelivery) ackWithRetry(ctx context.Context, errors chan<- error, errorCount int) error {
	for {
		switch err := delivery.Ack(); err {
		case nil, ErrorNotFound:
			return err
		default: // redis error
			errorCount++

			select { // try to add error to channel, but don't block
			case errors <- &DeliveryError{Delivery: delivery, RedisErr: err, Count: errorCount}:
			default:
			}

			if err := ctx.Err(); err != nil {
				return err
			}

			time.Sleep(time.Second)
		}
	}
}

func (delivery *wrapDelivery) RejectWithRetry(ctx context.Context, errors chan<- error) error {
	return delivery.moveWithRetry(ctx, errors, delivery.rejectedKey)
}

func (delivery *wrapDelivery) PushWithRetry(ctx context.Context, errors chan<- error) error {
	if delivery.pushKey == "" {
		return delivery.RejectWithRetry(ctx, errors) // fall back to rejecting
	}

	return delivery.moveWithRetry(ctx, errors, delivery.pushKey)
}

func (delivery *wrapDelivery) moveWithRetry(ctx context.Context, errors chan<- error, key string) error {
	errorCount := 0
	for {
		_, err := delivery.redisClient.LPush(key, delivery.payload)
		if err == nil { // success
			break
		}
		// error

		errorCount++

		select { // try to add error to channel, but don't block
		case errors <- &DeliveryError{Delivery: delivery, RedisErr: err, Count: errorCount}:
		default:
		}

		time.Sleep(time.Second)
	}

	return delivery.ackWithRetry(ctx, errors, errorCount)
}
