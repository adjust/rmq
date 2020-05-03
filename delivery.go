package rmq

import (
	"fmt"
)

type Delivery interface {
	Payload() string
	Ack() error
	Reject() error
	Push() error
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

func (delivery *wrapDelivery) Ack() error {
	// debug(fmt.Sprintf("delivery ack %s", delivery)) // COMMENTOUT

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
	if delivery.pushKey != "" {
		return delivery.move(delivery.pushKey)
	} else {
		return delivery.move(delivery.rejectedKey)
	}
}

func (delivery *wrapDelivery) move(key string) error {
	if _, err := delivery.redisClient.LPush(key, delivery.payload); err != nil {
		return err
	}

	count, err := delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrorNotFound
	}
	return nil
}
