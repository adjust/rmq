package main

import (
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/adjust/rmq"
)

const numProducers = 100000

const payload = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

func main() {
	runtime.GOMAXPROCS(12)

	connection := rmq.OpenConnection("producer", "tcp", "localhost:6379", 2)
	things := connection.OpenQueue("things")

	for i := 0; i < 10000; i++ {
		go func() {
			m := []int{}
			i := 0
			for {
				m = append(m, i)
				i++
				time.Sleep(time.Nanosecond)
			}
		}()
	}

	go stats()

	// start publishing infinitely
	for p := 0; p < numProducers; p++ {
		go func(p int) {
			batchSize := 0
			// before := time.Now()
			nextLog := time.Now().Add(100 * time.Millisecond)

			for d := 0; true; d++ {
				delivery := fmt.Sprintf("prod%ddel%d%s", p, d, payload)
				things.Publish(delivery)
				batchSize++

				// log stats
				if time.Now().After(nextLog) {
					// duration := time.Now().Sub(before)
					// perSecond := time.Second / (duration / time.Duration(batchSize))
					// log.Printf("producer %d produced %d %d", p, d, perSecond)

					// before = time.Now()
					batchSize = 0
					nextLog = time.Now().Add(100 * time.Millisecond)
				}
			}
		}(p)
	}

	// schedule publish buffer size changes
	time.Sleep(100 * time.Second)
	// for _, bufferSize := range []int{0, 1, 10, 10, 0, 100, 1000, 10000, 0, 100000, 0} {
	for _, bufferSize := range []int{10, 0, 100, 10, 0} {
		connection.SetPublishBufferSize(bufferSize, 100*time.Millisecond)
		log.Printf("=== %d ===", bufferSize)
		time.Sleep(time.Second)
	}
}

func stats() {
	memStats := &runtime.MemStats{}
	var lastPauseNs, lastMallocs, lastFrees uint64
	var lastNumGc uint32

	nsInMs := uint64(time.Millisecond)

	for {
		runtime.ReadMemStats(memStats)

		log.Printf("goroutines %d", runtime.NumGoroutine())
		stack := int(memStats.StackInuse)
		log.Printf("memory.stack %d", stack)

		log.Printf("memory.heap.alloc %d", int(memStats.HeapAlloc))
		log.Printf("memory.heap.objects %d", int(memStats.HeapObjects))

		mallocsSinceLastSample := memStats.Mallocs - lastMallocs
		log.Printf("memory.mallocs %d", int(mallocsSinceLastSample))
		lastMallocs = memStats.Mallocs

		freesSinceLastSample := memStats.Frees - lastFrees
		log.Printf("memory.frees %d", int(freesSinceLastSample))
		lastFrees = memStats.Frees

		pauseSinceLastSample := memStats.PauseTotalNs - lastPauseNs
		log.Printf("memory.gc.pause %d", int(pauseSinceLastSample/nsInMs))
		lastPauseNs = memStats.PauseTotalNs

		countGc := int(memStats.NumGC - lastNumGc)

		// if countGc > 0 {
		log.Printf("memory.gc.runs %d", countGc)
		// }

		lastNumGc = memStats.NumGC

		time.Sleep(1000 * time.Millisecond)
	}
}
