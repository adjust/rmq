package rmq

import (
	"fmt"
)

type Delivery interface {
	Payload() string
	Ack() (bool, error)
	Reject() (bool, error)
	Push() (bool, error)
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

func (delivery *wrapDelivery) Ack() (bool, error) {
	// debug(fmt.Sprintf("delivery ack %s", delivery)) // COMMENTOUT

	// TODO: check for other places where we get a returned affectedCount but
	// ignore it. consider checking the value
	count, err := delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload)
	// TODO: return error only, different error if count != 1?
	return count == 1, err
}

func (delivery *wrapDelivery) Reject() (bool, error) {
	return delivery.move(delivery.rejectedKey)
}

func (delivery *wrapDelivery) Push() (bool, error) {
	if delivery.pushKey != "" {
		return delivery.move(delivery.pushKey)
	} else {
		return delivery.move(delivery.rejectedKey)
	}
}

// TODO: return how long the key is afterwards
func (delivery *wrapDelivery) move(key string) (bool, error) {
	if _, err := delivery.redisClient.LPush(key, delivery.payload); err != nil {
		return false, err
	}

	if _, err := delivery.redisClient.LRem(delivery.unackedKey, 1, delivery.payload); err != nil {
		return false, err
	}

	// debug(fmt.Sprintf("delivery rejected %s", delivery)) // COMMENTOUT
	return true, nil
}
