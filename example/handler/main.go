package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/adjust/rmq/v4"
	"github.com/go-redis/redis/v8"
)

func main() {
	rc := rmq.NewRedisWrapper(
		redis.NewClient(
			&redis.Options{
				Addr: "localhost:6379",
				DB:   2,
			},
		),
	)

	errChan := make(chan error)

	go func() {
		for  {
			fmt.Printf("handle error: %v\n", <- errChan)
		}
	}()

	opts := []rmq.Option{
		rmq.WithTag("handler"),
		rmq.WithRedisClient(rc),
		rmq.WithNamespace("example-project"),
		rmq.WithErrChan(errChan),
	}

	connection, err := rmq.OpenConnectionWithOptions(opts...)
	if err != nil {
		panic(err)
	}

	http.Handle("/overview", NewHandler(connection))
	fmt.Printf("Handler listening on http://localhost:3333/overview\n")
	if err := http.ListenAndServe(":3333", nil); err != nil {
		panic(err)
	}
}

type Handler struct {
	connection rmq.Connection
}

func NewHandler(connection rmq.Connection) *Handler {
	return &Handler{connection: connection}
}

func (handler *Handler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	layout := request.FormValue("layout")
	refresh := request.FormValue("refresh")

	queues, err := handler.connection.GetOpenQueues()
	if err != nil {
		panic(err)
	}

	stats, err := handler.connection.CollectStats(queues)
	if err != nil {
		panic(err)
	}

	log.Printf("queue stats\n%s", stats)
	fmt.Fprint(writer, stats.GetHtml(layout, refresh))
}
