package rmq

type Deliveries []Delivery

func (deliveries Deliveries) Ack() int {
	failedCount := 0
	for _, delivery := range deliveries {
		if !delivery.Ack() {
			failedCount++
		}
	}
	return failedCount
}

func (deliveries Deliveries) Reject() int {
	failedCount := 0
	for _, delivery := range deliveries {
		if !delivery.Reject() {
			failedCount++
		}
	}
	return failedCount
}

func (deliveries Deliveries) Push() int {
	failedCount := 0
	for _, delivery := range deliveries {
		if !delivery.Push() {
			failedCount++
		}
	}
	return failedCount
}
