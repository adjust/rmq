[![Build Status](https://github.com/adjust/rmq/workflows/test/badge.svg)](https://github.com/adjust/rmq/actions?query=branch%3Amaster+workflow%3Atest)
[![GoDoc](https://pkg.go.dev/badge/github.com/adjust/rmq)](https://pkg.go.dev/github.com/adjust/rmq)

## Overview

rmq is short for Redis message queue. It's a message queue system written in Go
and backed by Redis.

## Basic Usage

Let's take a look at how to use rmq.

### Import

Of course you need to import rmq wherever you want to use it.

```go
import "github.com/adjust/rmq/v5"
```

### Connection

Before we get to queues, we first need to establish a connection. Each rmq
connection has a name (used in statistics) and Redis connection details
including which Redis database to use. The most basic Redis connection uses a
TCP connection to a given host and a port:

```go
connection, err := rmq.OpenConnection("my service", "tcp", "localhost:6379", 1, errChan)
```

It's also possible to access a Redis listening on a Unix socket:

```go
connection, err := rmq.OpenConnection("my service", "unix", "/tmp/redis.sock", 1, errChan)
```

For more flexible setup you can pass Redis options or create your own Redis client:

```go
connection, err := OpenConnectionWithRedisOptions("my service", redisOptions, errChan)
```

```go
connection, err := OpenConnectionWithRedisClient("my service", redisClient, errChan)
```

If the Redis instance can't be reached you will receive an error indicating this.

Please also note the `errChan` parameter. There is some rmq logic running in
the background which can run into Redis errors. If you pass an error channel to
the `OpenConnection()` functions rmq will send those background errors to this
channel so you can handle them asynchronously. For more details about this and
handling suggestions see the section about handling background errors below.

#### Connecting to a Redis cluster

In order to connect to a Redis cluster please use `OpenClusterConnection()`:

```go
redisClusterOptions := &redis.ClusterOptions{ /* ... */ }
redisClusterClient := redis.NewClusterClient(redisClusterOptions)
connection, err := OpenClusterConnection("my service", redisClusterClient, errChan)
```

Note that such an rmq cluster connection uses different Redis than rmq connections
opened by `OpenConnection()` or similar. If you have used a Redis instance
with `OpenConnection()` then it is NOT SAFE to reuse that rmq system by connecting
to it via `OpenClusterConnection()`. The cluster state won't be compatible and
this will likely lead to data loss.

If you've previously used `OpenConnection()` or similar you should only consider
using `OpenClusterConnection()` with a fresh Redis cluster.

### Queues

Once we have a connection we can use it to finally access queues. Each queue
must have a unique name by which we address it. Queues are created once they
are accessed. There is no need to declare them in advance. Here we open a queue
named "tasks":

```go
taskQueue, err := connection.OpenQueue("tasks")
```

Again, possibly Redis errors might be returned.

### Producers

An empty queue is boring, let's add some deliveries! Internally all deliveries
are saved to Redis lists as strings. This is how you can publish a string
payload to a queue:

```go
delivery := "task payload"
err := TaskQueue.Publish(delivery)
```

In practice, however, it's more common to have instances of some struct that we
want to publish to a queue. Assuming `task` is of some type like `Task`, this
is how to publish the JSON representation of that task:

```go
// create task
taskBytes, err := json.Marshal(task)
if err != nil {
    // handle error
}

err = taskQueue.PublishBytes(taskBytes)
```

For a full example see [`example/producer`][producer.go].

[producer.go]: example/producer/main.go

### Consumers

Now that our queue starts filling, let's add a consumer. After opening the
queue as before, we need it to start consuming before we can add consumers.

```go
err := taskQueue.StartConsuming(10, time.Second)
```

This sets the prefetch limit to 10 and the poll duration to one second. This
means the queue will fetch up to 10 deliveries at a time before giving them to
the consumers. To avoid idling consumers while the queues are full, the
prefetch limit should always be greater than the number of consumers you are
going to add. If the queue gets empty, the poll duration sets how long rmq will
wait before checking for new deliveries in Redis.

Once this is set up, we can actually add consumers to the consuming queue.

```go
taskConsumer := &TaskConsumer{}
name, err := taskQueue.AddConsumer("task-consumer", taskConsumer)
```

To uniquely identify each consumer internally rmq creates a random name with
the given prefix. For example in this case `name` might be
`task-consumer-WB1zaq`. This name is only used in statistics. 

In our example above the injected `taskConsumer` (of type `*TaskConsumer`) must
implement the `rmq.Consumer` interface. For example:

```go
func (consumer *TaskConsumer) Consume(delivery rmq.Delivery) {
    var task Task
    if err = json.Unmarshal([]byte(delivery.Payload()), &task); err != nil {
        // handle json error
        if err := delivery.Reject(); err != nil {
            // handle reject error
        }
        return
    }

    // perform task
    log.Printf("performing task %s", task)
    if err := delivery.Ack(); err != nil {
        // handle ack error
    }
}
```

First we unmarshal the JSON package found in the delivery payload. If this
fails we reject the delivery. Otherwise we perform the task and ack the
delivery.

If you don't actually need a consumer struct you can use `AddConsumerFunc`
instead and pass a consumer function which handles an `rmq.Delivery`:

```go
name, err := taskQueue.AddConsumerFunc(func(delivery rmq.Delivery) {
    // handle delivery and call Ack() or Reject() on it
})
```

Please note that `delivery.Ack()` and similar functions have a built-in retry
mechanism which will block your consumers in some cases. This is because
because failing to acknowledge a delivery is potentially dangerous. For details
see the section about background errors below.

For a full example see [`example/consumer`][consumer.go].

#### Consumer Lifecycle

As described above you can add consumers to a queue. For each consumer rmq
takes one of the prefetched unacked deliveries from the delivery channel and
passes it to the consumer's `Consume()` function. The next delivery will only
be passed to the same consumer once the prior `Consume()` call returns. So each
consumer will only be consuming a single delivery at any given time.

Furthermore each `Consume()` call is expected to call either `delivery.Ack()`,
`delivery.Reject()` or `delivery.Push()` (see below). If that's not the case
these deliveries will remain unacked and the prefetch goroutine won't make
progress after a while. So make sure you always call exactly one of those
functions in your `Consume()` implementations.

[consumer.go]: example/consumer/main.go

## Background Errors

It's recommended to inject an error channel into the `OpenConnection()`
functions. This section describes it's purpose and how you might use it to
monitor rmq background Redis errors.

There are three sources of background errors which rmq detects (and handles
internally):

1. The `OpenConnection()` functions spawn a goroutine which keeps a heartbeat
   Redis key alive. This is important so that the cleaner (see below) can tell
   which connections are still alive and must not be cleaned yet. If the
   heartbeat goroutine fails to update the heartbeat Redis key repeatedly foo
   too long the cleaner might clean up the connection prematurely. To avoid
   this the connection will automatically stop all consumers after 45
   consecutive heartbeat errors. This magic number is based on the details of
   the heartbeat key: The heartbeat tries to update the key every second with a
   TTL of one minute. So only after 60 failed attempts the heartbeat key would
   be dead.

   Every time this goroutine runs into a Redis error it gets send to the error
   channel as `HeartbeatError`.

2. The `StartConsuming()` function spawns a goroutine which is responsible for
   prefetching deliveries from the Redis `ready` list and moving them into a
   delivery channel. This delivery channels feeds into your consumers
   `Consume()` functions. If the prefetch goroutine runs into Redis errors this
   basically means that there won't be new deliveries being sent to your
   consumers until it can fetch new ones. So these Redis errors are not
   dangerous, it just means that your consumers will start idling until the
   Redis connection recovers.

   Every time this goroutine runs into a Redis error it gets send to the error
   channel as `ConsumeError`.

3. The delivery functions `Ack()`, `Reject()` and `Push()` have a built-in
   retry mechanism. This is because failing to acknowledge a delivery
   is potentially dangerous. The consumer has already handled the delivery, so
   if it can't ack it the cleaner might end up moving it back to the ready list
   so another consumer might end up consuming it again in the future, leading
   to double delivery.

   So if a delivery failed to be acked because of a Redis error the `Ack()`
   call will block and retry once a second until it either succeeds or until
   consuming gets stopped (see below). In the latter case the `Ack()` call will
   return `rmq.ErrorConsumingStopped` which you should handle in your consume
   function.  For example you might want to log about the delivery so you can
   manually remove it from the unacked or ready list before you start new
   consumers. Or at least you can know which deliveries might end up being
   consumed twice.

   Every time these functions runs into a Redis error it gets send to the error
   channel as `DeliveryError`.

Each of those error types has a field `Count` which tells you how often the
operation failed consecutively. This indicates for how long the affected Redis
instance has been unavailable. One general way of using this information might
be to have metrics about the error types including the error count so you can
keep track of how stable your Redis instances and connections are. By
monitoring this you might learn about instabilities before they affect your
services in significant ways.

Below is some more specific advice on handling the different error cases
outlined above. Keep in mind though that all of those errors are likely to
happen at the same time, as Redis tends to be up or down completely. But if
you're using multi Redis instance setup like [nutcracker][nutcracker] you might
see some of them in isolation from the others.

1. `HeartbeatErrors`: Once `err.Count` equals `HeartbeatErrorLimit` you should
   know that the consumers of this connection will stop consuming. And they
   won't restart consuming on their own. This is a condition you should closely
   monitor because this means you will have to restart your service in order to
   resume consuming. Before restarting you should check your Redis instance.

2. `ConsumeError`: These are mostly informational. As long as those errors keep
   happening the consumers will effectively be paused. But once these
   operations start succeeding again the consumers will resume consumers on
   their own.

3. `DeliveryError`: When you see deliveries failing to ack repeatedly this also
   means your consumers won't make progress as they will keep retrying to ack
   pending deliveries before starting to consume new ones. As long as this
   keeps happening you should avoid stopping the service if you can. That is
   because the already consumed by not yet unacked deliveries will be returned
   to `ready` be the cleaner afterwards, which leads to double delivery. So
   ideally you try to get Redis connection up again as long as the deliveries
   are still trying to ack. Once acking works again it's safe to restart again.

   More realistically, if you still need to stop the service when Redis is
   down, keep in mind that calling `StopConsuming()` will make the blocking
   `Ack()` calls return with `ErrorConsumingStopped`, so you can handle that
   case to make an attempt to either avoid the double delivery or at least
   track it for future investigation.

[nutcracker]: https://github.com/twitter/twemproxy

## Advanced Usage

### Batch Consumers

Sometimes it's useful to have consumers work on batches of deliveries instead
of individual ones. For example for bulk database inserts. In those cases you
can use `AddBatchConsumer()`:

```go
batchConsumer := &MyBatchConsumer{}
name, err := taskQueue.AddBatchConsumer("my-consumer", 100, time.Second, batchConsumer)
```

In this example we create a batch consumer which will receive batches of up to
100 deliveries. We set the `batchTimeout` to one second, so if there are less
than 100 deliveries per second we will still consume at least one batch per
second (which would contain less than 100 deliveries).

The `rmq.BatchConsumer` interface is very similar to `rmq.Consumer`.

```go
func (consumer *MyBatchConsumer) Consume(batch rmq.Deliveries) {
    payloads := batch.Payloads()
    // handle payloads
    if errors := batch.Ack(); len(errors) > 0 {
        // handle ack errors
    }
}
```

Note that `batch.Ack()` acknowledges all deliveries in the batch. It's also
possible to ack some of the deliveries and reject the rest. It uses the same
retry mechanism per delivery as discussed above. If some of the deliveries
continue to fail to ack when consuming gets stopped (see below), then
`batch.Ack()` will return an error map `map[int]error`. For each entry in this
map the key will be the index of the delivery which failed to ack and the value
will be the error it ran into. That way you can map the errors back to the
deliveries to know which deliveries are at risk of being consumed again in the
future as discussed above.

For a full example see [`example/batch_consumer`][batch_consumer.go].

[batch_consumer.go]: example/batch_consumer/main.go

### Push Queues

Another thing which can be useful is a mechanism for retries. Let's say you
have tasks which can fail for external reasons but you'd like to retry them a
few times after a while before you give up. In that case you can set up a chain
of push queues like this:

```
incomingQ -> pushQ1 -> pushQ2
```

In the queue setup code it would look like this (error handling omitted for
brevity):

```go
incomingQ, err := connection.OpenQueue("incomingQ")
pushQ1, err := connection.OpenQueue("pushQ1")
pushQ2, err := connection.OpenQueue("pushQ2")
incomingQ.SetPushQueue(pushQ1)
pushQ1.SetPushQueue(pushQ2)
_, err := incomingQ.AddConsumer("incomingQ", NewConsumer())
_, err := pushQ1.AddConsumer("pushQ1", NewConsumer())
_, err := pushQ2.AddConsumer("pushQ2", NewConsumer())
```

If you have set up your queues like this, you can now call `delivery.Push()` in
your `Consume()` function to push the delivery from the consuming queue to the
associated push queue. So if consumption fails on `incomingQ`, then the
delivery would be moved to `pushQ1` and so on. If you have the consumers wait
until the deliveries have a certain age you can use this pattern to retry after
certain durations.

Note that `delivery.Push()` has the same affect as `delivery.Reject()` if the
queue has no push queue set up. So in our example above, if the delivery fails
in the consumer on `pushQ2`, then the `Push()` call will reject the delivery.

### Stop Consuming

If you want to stop consuming from the queue, you can call `StopConsuming()`:

```go
finishedChan := taskQueue.StopConsuming()
```

When `StopConsuming()` is called, it will immediately stop fetching more
deliveries from Redis and won't send any more of the already prefetched
deliveries to consumers.

In the background it will make pending `Ack()` calls return
`rmq.ErrorConsumingStopped` if they still run into Redis errors (see above) and
wait for all consumers to finish consuming their current delivery before
closing the returned `finishedChan`. So while `StopConsuming()` returns
immediately, you can wait on the returned channel until all consumers are done:

```go
<-finishedChan
```

You can also stop consuming on all queues in your connection:

```go
finishedChan := connection.StopAllConsuming()
```

Wait on the `finishedChan` to wait for all consumers on all queues to finish.

This is useful to implement a graceful shutdown of a consumer service. Please
note that after calling `StopConsuming()` the queue might not be in a state
where you can add consumers and call `StartConsuming()` again. If you have a
use case where you actually need that sort of flexibility, please let us know.
Currently for each queue you are only supposed to call `StartConsuming()` and
`StopConsuming()` at most once.

### Return Rejected Deliveries

Even if you don't have a push queue setup there are cases where you need to
consume previously failed deliveries again. For example an external dependency
might have an issue or you might have deployed a broken consumer service which
rejects all deliveries for some reason.

In those cases you would wait for the external party to recover or fix your
mistake to get ready to reprocess the deliveries again. Now you can return the
deliveries by opening affected queue and call `ReturnRejected()`:

```go
returned, err := queue.ReturnRejected(10000)
```

In this case we ask rmq to return up to 10k deliveries from the `rejected` list
to the `ready` list. To return all of them you can pass `math.MaxInt64`.

If there was no error it returns the number of deliveries that were moved.

If you find yourself doing this regularly on some queues consider setting up a
push queue to automatically retry failed deliveries regularly.

See [`example/returner`][returner.go]

[returner.go]: example/returner/main.go

### Purge Rejected Deliveries

You might run into the case where you have rejected deliveries which you don't
intend to retry again for one reason or another. In those cases you can clear
the full `rejected` list by calling `PurgeRejected()`:

```go
count, err := queue.PurgeRejected()
```

It returns the number of purged deliveries.

Similarly, there's a function to clear the `ready` list of deliveries:

```go
count, err := queue.PurgeReady()
```

See [`example/purger`][purger.go].

[purger.go]: example/purger/main.go

### Cleaner

You should regularly run a queue cleaner to make sure no unacked deliveries are
stuck in the queue system. The background is that a consumer service prefetches
deliveries by moving them from the `ready` list to an `unacked` list associated
with the queue connection. If the consumer dies by crashing or even by being
gracefully shut down by calling `StopConsuming()`, the unacked deliveries will
remain in that Redis list.

If you run a queue cleaner regularly it will detect queue connections whose
heartbeat expired and will clean up all their consumer queues by moving their
unacked deliveries back to the `ready` list.

Although it should be safe to run multiple cleaners, it's recommended to run
exactly one instance per queue system and have it trigger the cleaning process
regularly, like once a minute.

See [`example/cleaner`][cleaner.go].

[cleaner.go]: example/cleaner/main.go

### Header

Redis protocol does not define a specific way to pass additional data like header.
However, there is often need to pass them (for example for traces propagation).

This implementation injects optional header values marked with a signature into 
payload body during publishing. When message is consumed, if signature is present, 
header and original payload are extracted from augmented payload.

Header is defined as `http.Header` for better interoperability with existing libraries,
for example with [`propagation.HeaderCarrier`](https://pkg.go.dev/go.opentelemetry.io/otel/propagation#HeaderCarrier).

```go
 // ....
 
 h := make(http.Header)
 h.Set("X-Baz", "quux")

 // You can add header to your payload during publish.
 _ = pub.Publish(rmq.PayloadWithHeader(`{"foo":"bar"}`, h))

 // ....

 _, _ = con.AddConsumerFunc("tag", func(delivery rmq.Delivery) {
     // And receive header back in consumer.
     delivery.(rmq.WithHeader).Header().Get("X-Baz") // "quux"
     
     // ....
 })
```

Adding a header is an explicit opt-in operation and so it does not affect library's
backwards compatibility by default (when not used). 

Please note that adding header may lead to compatibility issues if:
* consumer is built with older version of `rmq` when publisher has already 
   started using header, this can be avoided by upgrading consumers before publishers;
* consumer is not using `rmq` (other libs, low level tools like `redis-cli`) and is 
   not aware of payload format extension.

## Testing Included

To simplify testing of queue producers and consumers we include test mocks.

### Test Connection

As before, we first need a queue connection, but this time we use a
`rmq.TestConnection` that doesn't need any connection settings.

```go
testConn := rmq.NewTestConnection()
```

If you are using a testing framework that uses test suites, you can reuse that
test connection by setting it up once for the suite and resetting it with
`testConn.Reset()` before each test.

### Producer Tests

Now let's say we want to test the function `publishTask()` that creates a task
and publishes it to a queue from that connection.

```go
// call the function that should publish a task
publishTask(testConn)

// check that the task is published
assert.Equal(t, "task payload", suite.testConn.GetDelivery("tasks", 0))
```

The `assert.Equal` part is from [testify][testify], but it will look similar
for other testing frameworks. Given a `rmq.TestConnection`, we can check the
deliveries that were published to its queues (since the last `Reset()` call)
with `GetDelivery(queueName, index)`. In this case we want to extract the first
(and possibly only) delivery that was published to queue `tasks` and just check
the payload string.

If the payload is JSON again, the unmarshalling and check might look like this:

```go
var task Task
err := json.Unmarshal([]byte(suite.testConn.GetDelivery("tasks", 0)), &task)
assert.NoError(t, err)
assert.NotNil(t, task)
assert.Equal(t, "value", task.Property)
```

If you expect a producer to create multiple deliveries you can use different
indexes to access them all.

```go
assert.Equal(t, "task1", suite.testConn.GetDelivery("tasks", 0))
assert.Equal(t, "task2", suite.testConn.GetDelivery("tasks", 1))
```

For convenience there's also a function `GetDeliveries` that returns all
published deliveries to a queue as string array.

```go
assert.Equal(t, []string{"task1", "task2"}, suite.testConn.GetDeliveries("tasks"))
```

These examples assume that you inject the `rmq.Connection` into your testable
functions. If you inject instances of `rmq.Queue` instead, you can use
`rmq.TestQueue` instances in tests and access their `LastDeliveries` (since
`Reset()`) directly.

[testify]: https://github.com/stretchr/testify

### Consumer Tests

Testing consumers is a bit easier because consumers must implement the
`rmq.Consumer` interface. In the tests just create an `rmq.TestDelivery` and
pass it to your `Consume()` function. This example creates a test delivery from
a string and then checks that the delivery was acked.

```go
consumer := &TaskConsumer{}
delivery := rmq.NewTestDeliveryString("task payload")

consumer.Consume(delivery)

assert.Equal(t, rmq.Acked, delivery.State)
```

The `State` field will always be one of these values:

- `rmq.Acked`: The delivery was acked
- `rmq.Rejected`: The delivery was rejected
- `rmq.Pushed`: The delivery was pushed (see below)
- `rmq.Unacked`: Nothing of the above

If your packages are JSON marshalled objects, then you can create test
deliveries out of those like this:

```go
task := Task{Property: "bad value"}
delivery := rmq.NewTestDelivery(task)
```

### Integration Tests

If you want to write integration tests which exercise both producers and
consumers at the same time, you can use the
`rmq.OpenConnectionWithTestRedisClient` constructor. It returns a real
`rmq.Connection` instance which is backed by an in-memory Redis client
implementation. That way it behaves exactly as in production, just without the
durability of a real Redis client. Don't use this in production!

## Statistics

Given a connection, you can call `connection.CollectStats()` to receive
`rmq.Stats` about all open queues, connections and consumers. If you run
[`example/handler`][handler.go] you can see what's available:

<img width="610" src="https://user-images.githubusercontent.com/474504/82765106-1c53a600-9e14-11ea-8c30-e96821afa0d8.png">

In this example you see 5 connections consuming `task_kind1`, each wich 5
consumers each. They have a total of 1007 packages unacked. Below the marker you see
connections which are not consuming. One of the handler connections died
because I stopped the handler. Running the cleaner would clean that up (see
below).

[handler.go]: example/handler/main.go

### Prometheus

If you are using Prometheus, [rmqprom](https://github.com/pffreitas/rmqprom)
collects statistics about all open queues and exposes them as Prometheus
metrics.
