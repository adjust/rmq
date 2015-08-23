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
