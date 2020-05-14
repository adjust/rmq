[![Build Status](https://travis-ci.org/adjust/rmq.svg?branch=master)](https://travis-ci.org/adjust/rmq)
[![GoDoc](https://godoc.org/github.com/adjust/rmq?status.svg)](https://godoc.org/github.com/adjust/rmq)

---

**Note**: We recently updated rmq to use latest go-redis client [`github.com/go-redis/redis`][go-redis].
If you don't want to upgrade yet, you can continue using rmq branch `v1`:

1. Using a dependency manager, i.e. [dep][]

   ```toml
   # Gopkg.toml
   [[constraint]]
     name = "github.com/adjust/rmq"
     branch = "v1"
   ```

2. Using [gopkg.in][gopkg]

   ```go
   import "gopkg.in/adjust/rmq.v1"
   ```

   See https://gopkg.in/adjust/rmq.v1

[go-redis]: https://github.com/go-redis/redis
[dep]: https://golang.github.io/dep/
[gopkg]: https://gopkg.in

---

## Overview

rmq is short for Redis message queue. It's a message queue system written in Go
and backed by Redis. It's similar to [redismq][redismq], but implemented
independently with a different interface in mind.

[redismq]: https://github.com/adjust/redismq

## Basic Usage

Lets take a look at how to use rmq.

### Import

Of course you need to import rmq wherever you want to use it.

```go
import "github.com/adjust/rmq"
```

### Connection

Before we get to queues, we first need to establish a connection. Each rmq
connection has a name (used in statistics) and Redis connection details
including which Redis database to use. The most basic Redis connection uses a
TCP connection to a given host and a port.

```go
connection := rmq.OpenConnection("my service", "tcp", "localhost:6379", 1)
```

But it's also possible to access a Redis listening on a Unix socket.

```go
connection := rmq.OpenConnection("my service", "unix", "/tmp/redis.sock", 1)
```

Note: rmq panics on Redis connection errors. Your producers and consumers will
crash if Redis goes down. Please let us know if you would see this handled
differently.

### Queue

Once we have a connection we can use it to finally access queues. Each queue
must have a unique name by which we address it. Queues are created once they
are accessed. There is no need to declare them in advance. Here we open a queue
named "tasks":

```go
taskQueue := connection.OpenQueue("tasks")
```

### Producer

An empty queue is boring, lets add some deliveries! Internally all deliveries
are saved to Redis as strings. This is how you can publish a string payload to
a queue:

```go
delivery := "task payload"
taskQueue.Publish(delivery)
```

In practice, however, it's more common to have instances of some struct that we
want to publish to a queue. Assuming `task` is of some type like `Kind`, this is
how to publish the JSON representation of that task:

```go
// create task
taskBytes, err := json.Marshal(task)
if err != nil {
    // handle error
    return
}

taskQueue.PublishBytes(taskBytes)
```

For a full example see [`example/producer`][producer.go]

[producer.go]: example/producer/main.go

### Consumer

Now that our queue starts filling, lets add a consumer. After opening the queue
as before, we need it to start consuming before we can add consumers.

```go
taskQueue.StartConsuming(10, time.Second)
```

This sets the prefetch limit to 10 and the poll duration to one second. This
means the queue will fetch up to 10 deliveries at a time before giving them to
the consumers. To avoid idling producers in times of full queues, the prefetch
limit should always be greater than the number of consumers you are going to
add. If the queue gets empty, the poll duration sets how long to wait before
checking for new deliveries in Redis.

Once this is set up, we can actually add consumers to the consuming queue.

```go
taskConsumer := &TaskConsumer{}
taskQueue.AddConsumer("task consumer", taskConsumer)
```

For our example this assumes that you have a struct `TaskConsumer` that
implements the `rmq.Consumer` interface like this:

```go
func (consumer *TaskConsumer) Consume(delivery rmq.Delivery) {
    var task Task
    if err = json.Unmarshal([]byte(delivery.Payload()), &task); err != nil {
        // handle error
        delivery.Reject()
        return
    }

    // perform task
    log.Printf("performing task %s", task)
    delivery.Ack()
}
```

First we unmarshal the JSON package found in the delivery payload. If this fails
we reject the delivery, otherwise we perform the task and ack the delivery.

If you don't actually need a consumer struct you can just call `AddConsumerFunc`
instead and pass in a consumer function which directly handles an `rmq.Delivery`:

```go
taskQueue.AddConsumerFunc(func(delivery rmq.Delivery) {
    // handle delivery and call Ack() or Reject() on it
})
```

For a full example see [`example/consumer`][consumer.go]

[consumer.go]: example/consumer/main.go

### Stop Consuming

If you want to stop consuming from the queue, you can call `StopConsuming`:

```go
finishedChan := taskQueue.StopConsuming()
```

When `StopConsuming` is called, it will continue fetching its current batch of
deliveries. After that the consumers continue to consume the fetched deliveries
until all unacked deliveries are fully consumed. If `StopConsuming` is called
before consuming or after already stopped, it will return a closed channel. If
you want to wait until all consumers are idle you can wait on the `finishedChan`:

```go
  <-finishedChan
```

This is useful to implement a graceful shutdown of a consumer service. Please
note that after calling `StopConsuming` the queue might not be in a state where
you can add consumers and call `StartConsuming` again. If you have a use case
where you actually need that sort of flexibility, please let us know. Currently
for each queue you are only supposed to call `StartConsuming` and
`StopConsuming` at most once.

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
`testConn.Reset` before each test.

### Producer Test

Now lets say we want to test the function `publishTask` that creates a task and
publishes it to a queue from that connection.

```go
// call the function that should publish a task
publishTask(testConn)

// check that the task is published
c.Check(suite.testConn.GetDelivery("tasks", 0), Equals, "task payload")
```

The `c.Check` part is from [gocheck][gocheck], but it will look similar for
other testing frameworks. Given a `rmq.TestConnection`, we can check the
deliveries that were published to it's queues (since the last `Reset()` call)
with `GetDelivery(queue, index)`. In this case we want to extract the first
(and possibly only) delivery that was published to queue `tasks` and just check
the payload string.

If the payload is JSON again, the unmarshalling and check might look like this:

```go
var task Task
err := json.Unmarshal([]byte(suite.testConn.GetDelivery("tasks", 0)), &task)
c.Assert(err, IsNil)
c.Assert(task, NotNil)
c.Check(task.Property, Equals, "value")
```

If you expect a producer to create multiple deliveries you can use different
indexes to access them all.

```go
c.Check(suite.testConn.GetDelivery("tasks", 0), Equals, "task1")
c.Check(suite.testConn.GetDelivery("tasks", 1), Equals, "task2")
```

For convenience there's also a function `GetDeliveries` that returns all
published deliveries to a queue as string array.

```go
c.Check(suite.testConn.GetDeliveries("tasks"), DeepEquals, []string{"task1", "task2"})
```

If your producer doesn't have guarantees about the order of its deliveries, you
could implement a selector function like `findByPrefix` and then check each
delivery regardless of their index.

```go
tasks := suite.testConn.GetDeliveries("tasks")
c.Assert(tasks, HasLen, 2)
xTask := findByPrefix(tasks, "x")
yTask := findByPrefix(tasks, "y")
c.Check(xTask.Id, Equals, "3")
c.Check(yTask.Id, Equals, "4")
```

These examples assumed that you inject the `rmq.Connection` into your testable
functions. If you inject instances of `rmq.Queue` instead, you can use
`rmq.TestQueue` instances in tests and access their `LastDeliveries` (since
`Reset()`) directly.

[gocheck]: https://labix.org/gocheck

### Consumer Test

Testing consumers is a bit easier because consumers must implement the
`rmq.Consumer` interface. In the tests just create `rmq.TestDelivery` and pass
it to your `Consume` implementation. This example creates a test delivery from a
string and then checks that the delivery was acked.

```go
consumer := &TaskConsumer{}
delivery := rmq.NewTestDeliveryString("task payload")

consumer.Consume(delivery)

c.Check(delivery.State, Equals, rmq.Acked)
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

### Integration tests

If you want to write integration tests which exercise both producers and
consumers at the same time, you can use the `OpenConnectionWithTestRedisClient`
constructor. It returns a real `rmq.Connection` instance which is backed by an
in memory Redis client implementation. That way it behaves exactly as in
production, just without the durability of a real Redis client. Don't use this
in production!

## Statistics

Given a connection, you can call `connection.CollectStats` to receive
`rmq.Stats` about all open queues, connections and consumers. If you run
[`example/handler`][handler.go] you can see what's available:

![][handler.png]

In this example you see three connections consuming `things`, each wich 10
consumers each. Two of them have 8 packages unacked. Below the marker you see
connections which are not consuming. One of the handler connections died
because I stopped the handler. Running the cleaner would clean that up (see
below).

[handler.go]: example/handler/main.go
[handler.png]: http://i.imgur.com/5FexMvZ.png

### Prometheus

If you are using Prometheus, [rmqprom](https://github.com/pffreitas/rmqprom) collects statistics about all open queues and exposes them as Prometheus metrics.

## TODO

There are some features and aspects not properly documented yet. I will quickly
list those here, hopefully they will be expanded in the future. :wink:

- Batch Consumers: Use `queue.AddBatchConsumer()` to register a consumer that
  receives batches of deliveries to be consumed at once (database bulk insert)
  See [`example/batch_consumer`][batch_consumer.go]
- Push Queues: When consuming queue A you can set up its push queue to be queue
  B. The consumer can then call `delivery.Push()` to push this delivery
  (originally from queue A) to the associated push queue B. (useful for
  retries)
- Cleaner: Run this regularly to return unacked deliveries of stopped or
  crashed consumers back to ready so they can be consumed by a new consumer.
  See [`example/cleaner`][cleaner.go]
- Returner: Imagine there was some error that made you reject a lot of
  deliveries by accident. Just call `queue.ReturnRejected()` to return all
  rejected deliveries of that queue back to ready. (Similar to `ReturnUnacked`
  which is used by the cleaner) Consider using push queues if you do this
  regularly. See [`example/returner`][returner.go]
- Purger: If deliveries failed you don't want to retry them anymore for whatever
  reason, you can call `queue.PurgeRejected()` to dispose of them for good.
  There's also `queue.PurgeReady` if you want to get a queue clean without
  consuming possibly bad deliveries. See [`example/purger`][purger.go]

[batch_consumer.go]: example/batch_consumer/main.go
[cleaner.go]: example/cleaner/main.go
[returner.go]: example/returner/main.go
[purger.go]: example/purger/main.go
