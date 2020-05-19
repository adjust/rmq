package rmq

import (
	"context"
	"fmt"
	"time"
)

type Delivery interface {
	Payload() string

	Ack(context.Context) error
	Reject(context.Context) error
	Push(context.Context) error
}

type redisDelivery struct {
	payload     string
	unackedKey  string
	rejectedKey string
	pushKey     string
	redisClient RedisClient
	errChan     chan<- error
}

func newDelivery(
	payload string,
	unackedKey string,
	rejectedKey string,
	pushKey string,
	redisClient RedisClient,
	errChan chan<- error,
) *redisDelivery {
	return &redisDelivery{
		payload:     payload,
		unackedKey:  unackedKey,
		rejectedKey: rejectedKey,
		pushKey:     pushKey,
		redisClient: redisClient,
		errChan:     errChan,
	}
}

func (delivery *redisDelivery) String() string {
	return fmt.Sprintf("[%s %s]", delivery.payload, delivery.unackedKey)
}

func (delivery *redisDelivery) Payload() string {
	return delivery.payload
}

// blocking versions of the functions below with the following behavior:
// 1. return immediately if the operation succeeded or failed with ErrorNotFound
// 2. in case of other redis errors, send them to the errors chan and retry after a sleep
// 3. if the context is cancalled or its timeout exceeded, context.Cancelled or
//    context.DeadlineExceeded will be returned

func (delivery *redisDelivery) Ack(ctx context.Context) error {
	if ctx == nil { // TODO: remove this
		ctx = context.TODO()
	}

	errorCount := 0
	for {
		count, err := delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)
		if err == nil { // no redis error
			if count == 0 {
				return ErrorNotFound
			}
			return nil
		}

		// redis error

		errorCount++

		select { // try to add error to channel, but don't block
		case delivery.errChan <- &DeliveryError{Delivery: delivery, RedisErr: err, Count: errorCount}:
		default:
		}

		if err := ctx.Err(); err != nil {
			return err
		}

		time.Sleep(time.Second)
	}
}

func (delivery *redisDelivery) Reject(ctx context.Context) error {
	return delivery.move(ctx, delivery.rejectedKey)
}

func (delivery *redisDelivery) Push(ctx context.Context) error {
	if delivery.pushKey == "" {
		return delivery.Reject(ctx) // fall back to rejecting
	}

	return delivery.move(ctx, delivery.pushKey)
}

func (delivery *redisDelivery) move(ctx context.Context, key string) error {
	errorCount := 0
	for {
		_, err := delivery.redisClient.LPush(key, delivery.payload)
		if err == nil { // success
			break
		}
		// error

		errorCount++

		select { // try to add error to channel, but don't block
		case delivery.errChan <- &DeliveryError{Delivery: delivery, RedisErr: err, Count: errorCount}:
		default:
		}

		time.Sleep(time.Second)
	}

	return delivery.Ack(ctx)
}

// lower level functions which don't retry but just return the first error
