package queue

type Delivery interface {
	Payload() string
}

type wrapDelivery struct {
	payload string
}

func newDelivery(payload string) *wrapDelivery {
	return &wrapDelivery{
		payload: payload,
	}
}

func (delivery *wrapDelivery) Payload() string {
	return delivery.payload
}
