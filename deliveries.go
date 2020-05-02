package rmq

type Deliveries []Delivery

func (deliveries Deliveries) Ack() (errors []error) {
	for _, delivery := range deliveries {
		if err := delivery.Ack(); err != nil {
			errors = append(errors, err)
		}
	}
	return errors
}

func (deliveries Deliveries) Reject() (errors []error) {
	for _, delivery := range deliveries {
		if err := delivery.Reject(); err != nil {
			errors = append(errors, err)
		}
	}
	return errors
}

func (deliveries Deliveries) Push() (errors []error) {
	for _, delivery := range deliveries {
		if err := delivery.Push(); err != nil {
			errors = append(errors, err)
		}
	}
	return errors
}
