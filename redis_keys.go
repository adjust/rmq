package rmq

import "strings"

const (
	connectionsKey                       = "rmq::connections"                                           // Set of connection names
	connectionHeartbeatTemplate          = "rmq::connection::{connection}::heartbeat"                   // expires after {connection} died
	connectionQueuesTemplate             = "rmq::connection::{connection}::queues"                      // Set of queues consumers of {connection} are consuming
	connectionQueueConsumersBaseTemplate = "rmq::connection::{connection}::queue::[{queue}]::consumers" // Set of all consumers from {connection} consuming from {queue}
	connectionQueueUnackedBaseTemplate   = "rmq::connection::{connection}::queue::[{queue}]::unacked"   // List of deliveries consumers of {connection} are currently consuming

	queuesKey                 = "rmq::queues"                     // Set of all open queues
	queueReadyBaseTemplate    = "rmq::queue::[{queue}]::ready"    // List of deliveries in that {queue} (right is first and oldest, left is last and youngest)
	queueRejectedBaseTemplate = "rmq::queue::[{queue}]::rejected" // List of rejected deliveries from that {queue}

	phConnection = "{connection}" // connection name
	phQueue      = "{queue}"      // queue name
	phConsumer   = "{consumer}"   // consumer name (consisting of tag and token)
)

func getTemplate(baseTemplate string, useRedisHashTags bool) string {
	if !useRedisHashTags {
		return baseTemplate
	}

	return strings.Replace(baseTemplate, "[{queue}]", "{{queue}}", 1)
}
