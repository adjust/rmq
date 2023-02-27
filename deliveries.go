package rmq

import "context"

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

func (deliveries Deliveries) Ack(ctx context.Context) (errMap map[int]error) {
	return deliveries.each(ctx, Delivery.Ack)
}

func (deliveries Deliveries) Reject(ctx context.Context) (errMap map[int]error) {
	return deliveries.each(ctx, Delivery.Reject)
}

func (deliveries Deliveries) Push(ctx context.Context) (errMap map[int]error) {
	return deliveries.each(ctx, Delivery.Push)
}

// helper functions

func (deliveries Deliveries) each(
	ctx context.Context, f func(Delivery, context.Context) error,
) (errMap map[int]error) {
	for i, delivery := range deliveries {
		if err := f(delivery, ctx); err != nil {
			if errMap == nil { // create error map lazily on demand
				errMap = map[int]error{}
			}
			errMap[i] = err
		}
	}
	return errMap
}
