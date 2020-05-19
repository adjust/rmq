package rmq

type Deliveries []Delivery

func (deliveries Deliveries) Payloads() []string {
	payloads := make([]string, len(deliveries))
	for i, delivery := range deliveries {
		payloads[i] = delivery.Payload()
	}
	return payloads
}

// NOTE: The returned error map maps delivery indexes to errors. So if the
// error map is non empty you can use the indexes in the map to look up which
// of the deliveries ran into the corresponding error. See
// example/batch_consumer.

// functions with retry, see comments in delivery.go (recommended)

func (deliveries Deliveries) Ack() (errMap map[int]error) {
	return deliveries.each(Delivery.Ack)
}

func (deliveries Deliveries) Reject() (errMap map[int]error) {
	return deliveries.each(Delivery.Reject)
}

func (deliveries Deliveries) Push() (errMap map[int]error) {
	return deliveries.each(Delivery.Push)
}

// helper functions

func (deliveries Deliveries) each(
	f func(Delivery) error,
) (errMap map[int]error) {
	for i, delivery := range deliveries {
		if err := f(delivery); err != nil {
			if errMap == nil { // create error map lazily on demand
				errMap = map[int]error{}
			}
			errMap[i] = err
		}
	}
	return errMap
}
