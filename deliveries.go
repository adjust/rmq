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

// TODO: map returned errors to deliveries to make clear which ones failed

// functions with retry, see comments in delivery.go (recommended)

func (deliveries Deliveries) AckWithRetry(ctx context.Context, errChan chan<- error) (errors []error) {
	return deliveries.eachWithRetry(ctx, errChan, Delivery.AckWithRetry)
}

func (deliveries Deliveries) RejectWithRetry(ctx context.Context, errChan chan<- error) (errors []error) {
	return deliveries.eachWithRetry(ctx, errChan, Delivery.RejectWithRetry)
}

func (deliveries Deliveries) PushWithRetry(ctx context.Context, errChan chan<- error) (errors []error) {
	return deliveries.eachWithRetry(ctx, errChan, Delivery.PushWithRetry)
}

// functions without retry, see comments in delivery.go (only use when you know what you are doing)

func (deliveries Deliveries) Ack() (errors []error) {
	return deliveries.each(Delivery.Ack)
}

func (deliveries Deliveries) Reject() (errors []error) {
	return deliveries.each(Delivery.Reject)
}

func (deliveries Deliveries) Push() (errors []error) {
	return deliveries.each(Delivery.Push)
}

// helper functions

func (deliveries Deliveries) eachWithRetry(
	ctx context.Context,
	errChan chan<- error,
	f func(Delivery, context.Context, chan<- error) error,
) (errors []error) {
	return deliveries.each(func(delivery Delivery) error {
		return f(delivery, ctx, errChan)
	})
}

func (deliveries Deliveries) each(f func(Delivery) error) (errors []error) {
	for _, delivery := range deliveries {
		if err := f(delivery); err != nil {
			errors = append(errors, err)
		}
	}
	return errors
}
