package main

import (
	"sync"
)

// Generator function to send datapoints in the xs channel
func genX(points int, xs chan<- int64) {
	for i := 0; i < points; i++ {
		xs <- int64(i)
	}
	close(xs)
}

// A worker function that reads datapoints from the xs channel and
// performs a mathematical calculation
func f(xs <-chan int64, yi chan<- int64, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		x, ok := <-xs
		if !ok {
			break // channel is closed
		}
		yi <- x * x
	}
}

// A worker function to aggregate all resuls
func aggregate(yi <-chan int64, result chan<- int64) {
	var sum int64
	for n := range yi {
		sum += n
	}
	result <- sum
}

// Calculates the sum of all squares of a number of points using goroutines
//
// This is just for example's sake, calculating the same result without goroutines is
// actually a bit faster compared to this approach. Though it's a good exaple of what
// we need to distribute the effort across multiple goroutines
func SquareSum(points int, numWorkers int) int64 {

	workers := numWorkers
	if workers <= 0 {
		workers = 1
	}

	xi := make(chan int64, workers)
	yi := make(chan int64, workers)
	resultCh := make(chan int64)
	defer close(resultCh)

	go aggregate(yi, resultCh) // aggregator routine

	wg := new(sync.WaitGroup)
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go f(xi, yi, wg) // spawn all function workers
	}

	genX(points, xi) // generator routine

	wg.Wait()
	close(yi)

	return <-resultCh
}
