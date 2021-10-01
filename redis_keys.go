package rmq

import "strings"

const (
	connectionsKey                   = "rmq::connections"                                           // Set of connection names
	connectionHeartbeatTemplate      = "rmq::connection::{connection}::heartbeat"                   // expires after {connection} died
	connectionQueuesTemplate         = "rmq::connection::{connection}::queues"                      // Set of queues consumers of {connection} are consuming
	connectionQueueConsumersTemplate = "rmq::connection::{connection}::queue::[{queue}]::consumers" // Set of all consumers from {connection} consuming from {queue}
	connectionQueueUnackedTemplate   = "rmq::connection::{connection}::queue::[{queue}]::unacked"   // List of deliveries consumers of {connection} are currently consuming

	queuesKey             = "rmq::queues"                     // Set of all open queues
	queueReadyTemplate    = "rmq::queue::[{queue}]::ready"    // List of deliveries in that {queue} (right is first and oldest, left is last and youngest)
	queueRejectedTemplate = "rmq::queue::[{queue}]::rejected" // List of rejected deliveries from that {queue}

	phConnection = "{connection}" // connection name
	phQueue      = "{queue}"      // queue name
	phConsumer   = "{consumer}"   // consumer name (consisting of tag and token)
)

type keys struct {
	namespace string
}

func (k keys) connectionsKey() string {
	return namespaced(k.namespace, connectionsKey)
}

func (k keys) connectionHeartbeat(connection string) string {
	return strings.Replace(namespaced(k.namespace, connectionHeartbeatTemplate), phConnection, connection, 1)
}

func (k keys) connectionQueues(connection string) string {
	return strings.Replace(namespaced(k.namespace, connectionQueuesTemplate), phConnection, connection, 1)
}

func (k keys) connectionQueueConsumers(connection, queue string) string {
	s := strings.Replace(namespaced(k.namespace, connectionQueueConsumersTemplate), phConnection, connection, 1)
	s = strings.Replace(s, phQueue, queue, 1)

	return s
}

func (k keys) connectionQueueUnacked(connection, queue string) string {
	s := strings.Replace(namespaced(k.namespace, connectionQueueUnackedTemplate), phConnection, connection, 1)
	s = strings.Replace(s, phQueue, queue, 1)

	return s
}

func (k keys) queuesKey() string {
	return namespaced(k.namespace, queuesKey)
}

func (k keys) queueReady(queue string) string {
	return strings.Replace(namespaced(k.namespace, queueReadyTemplate), phQueue, queue, 1)
}

func (k keys) queueRejected(queue string) string {
	return strings.Replace(namespaced(k.namespace, queueRejectedTemplate), phQueue, queue, 1)
}

func namespaced(namespace, key string) string {
	if namespace == "" {
		return key
	}

	return namespace + "::" + key
}
