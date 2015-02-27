package queue

import (
	"fmt"
	redis "github.com/adjust/redis-latest-head" // TODO: update
)

type Delivery interface {
	Payload() string
	Ack() bool
	Reject() bool
}

type wrapDelivery struct {
	payload     string
	unackedKey  string
	rejectedKey string
	redisClient *redis.Client
}

func newDelivery(payload, unackedKey, rejectedKey string, redisClient *redis.Client) *wrapDelivery {
	return &wrapDelivery{
		payload:     payload,
		unackedKey:  unackedKey,
		rejectedKey: rejectedKey,
		redisClient: redisClient,
	}
}

func (delivery *wrapDelivery) String() string {
	return fmt.Sprintf("[%s %s]", delivery.payload, delivery.unackedKey)
}

func (delivery *wrapDelivery) Payload() string {
	return delivery.payload
}

func (delivery *wrapDelivery) Ack() bool {
	// debug(fmt.Sprintf("delivery ack %s", delivery)) // COMMENTOUT

	result := delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)
	if redisErrIsNil(result) {
		return false
	}

	return result.Val() == 1
}

func (delivery *wrapDelivery) Reject() bool {
	if redisErrIsNil(delivery.redisClient.LPush(delivery.rejectedKey, delivery.payload)) {
		return false
	}

	if redisErrIsNil(delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)) {
		return false
	}

	// debug(fmt.Sprintf("delivery rejected %s", delivery)) // COMMENTOUT
	return true
}
