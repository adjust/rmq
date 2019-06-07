package rmq

type Deliveries []Delivery

func (deliveries Deliveries) Ack() (int, error) {
	failedCount := 0
	for _, delivery := range deliveries {
		ok, err := delivery.Ack()
		if err != nil {
			return 0, err
		}
		if !ok {
			failedCount++
		}
	}
	return failedCount, nil
}

func (deliveries Deliveries) Reject() (int, error) {
	failedCount := 0
	for _, delivery := range deliveries {
		ok, err := delivery.Reject()
		if err != nil {
			return 0, err
		}
		if !ok {
			failedCount++
		}
	}
	return failedCount, nil
}

func (deliveries Deliveries) Push() (int, error) {
	failedCount := 0
	for _, delivery := range deliveries {
		ok, err := delivery.Push()
		if err != nil {
			return 0, err
		}
		if !ok {
			failedCount++
		}
	}
	return failedCount, nil
}
