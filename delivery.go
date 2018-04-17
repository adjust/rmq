package rmq

import (
	"fmt"
)

type Delivery interface {
	Payload() string
	Ack() bool
	Reject() bool
	Push() bool
}

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

func (delivery *wrapDelivery) Ack() bool {
	// debug(fmt.Sprintf("delivery ack %s", delivery)) // COMMENTOUT

	count, ok := delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)
	return ok && count == 1
}

func (delivery *wrapDelivery) Reject() bool {
	return delivery.move(delivery.rejectedKey)
}

func (delivery *wrapDelivery) Push() bool {
	if delivery.pushKey != "" {
		return delivery.move(delivery.pushKey)
	} else {
		return delivery.move(delivery.rejectedKey)
	}
}

func (delivery *wrapDelivery) move(key string) bool {
	if ok := delivery.redisClient.LPush(key, delivery.payload); !ok {
		return false
	}

	if _, ok := delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload); !ok {
		return false
	}

	// debug(fmt.Sprintf("delivery rejected %s", delivery)) // COMMENTOUT
	return true
}
