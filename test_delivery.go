package rmq

import "encoding/json"

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
		bytes = []byte("rmq.NewTestDelivery failed to marshal")
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

func (delivery *TestDelivery) Ack() bool {
	if delivery.State == Unacked {
		delivery.State = Acked
		return true
	}
	return false
}

func (delivery *TestDelivery) Reject() bool {
	if delivery.State == Unacked {
		delivery.State = Rejected
		return true
	}
	return false
}

func (delivery *TestDelivery) Push() bool {
	if delivery.State == Unacked {
		delivery.State = Pushed
		return true
	}
	return false
}
