package rmq

import (
	"encoding/json"
	"log"
)

type TestDelivery struct {
	State   State
	payload string
}

func NewTestDelivery(content interface{}) *TestDelivery {
	if payload, ok := content.(string); ok {
		return NewTestDeliveryString(payload)
	}

	bytes, err := json.Marshal(content)
	if err != nil {
		log.Panic("rmq.NewTestDelivery failed to marshal")
	}

	return NewTestDeliveryString(string(bytes))
}

func NewTestDeliveryString(payload string) *TestDelivery {
	return &TestDelivery{
		payload: payload,
	}
}

func (delivery *TestDelivery) Payload() string {
	return delivery.payload
}

func (delivery *TestDelivery) Ack() error {
	if delivery.State != Unacked {
		return ErrorNotFound
	}
	delivery.State = Acked
	return nil
}

func (delivery *TestDelivery) Reject() error {
	if delivery.State != Unacked {
		return ErrorNotFound
	}
	delivery.State = Rejected
	return nil
}

func (delivery *TestDelivery) Push() error {
	if delivery.State != Unacked {
		return ErrorNotFound
	}
	delivery.State = Pushed
	return nil
}
