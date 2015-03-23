package queue

type state int

const (
	Unacked state = iota
	Acked
	Rejected
)

type TestDelivery struct {
	State   state
	payload string
}

func NewTestDelivery(payload string) *TestDelivery {
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
