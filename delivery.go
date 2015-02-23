package queue

import (
	"fmt"
	redis "github.com/adjust/redis-latest-head" // TODO: update
	"log"
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
	if result.Err() != nil {
		log.Printf("delivery failed to ack LRem failed %s", delivery)
		return false
	}
	if result.Val() == 0 {
		log.Printf("delivery failed to ack LRem none %s", delivery)
		return false
	}

	return true
}

func (delivery *wrapDelivery) Reject() bool {
	result := delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)
	if result.Err() != nil {
		log.Printf("delivery failed to reject LRem failed %s", delivery)
		return false
	}
	if result.Val() == 0 {
		log.Printf("delivery failed to reject LRem none %s", delivery)
		return false
	}

	result = delivery.redisClient.LPush(delivery.rejectedKey, delivery.payload)
	if result.Err() != nil {
		log.Printf("delivery failed to reject LPush failed %s", delivery)
		return false
	}
	if result.Val() == 0 {
		log.Printf("delivery failed to reject LPush failed %s", delivery)
		return false
	}

	// debug(fmt.Sprintf("delivery rejected %s", delivery)) // COMMENTOUT
	return true
}
