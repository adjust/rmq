package queue

import (
	redis "github.com/adjust/redis-latest-head" // TODO: update
)

type Delivery interface {
	Payload() string
	Ack() bool
}

type wrapDelivery struct {
	payload     string
	unackedKey  string
	redisClient *redis.Client
}

func newDelivery(payload, unackedKey string, redisClient *redis.Client) *wrapDelivery {
	return &wrapDelivery{
		payload:     payload,
		unackedKey:  unackedKey,
		redisClient: redisClient,
	}
}

func (delivery *wrapDelivery) Payload() string {
	return delivery.payload
}

func (delivery *wrapDelivery) Ack() bool {
	result := delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)
	if result.Err() != nil {
		return false
	}
	return result.Val() > 0
}
