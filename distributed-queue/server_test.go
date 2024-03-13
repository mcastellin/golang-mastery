package main

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func TestApiServer(t *testing.T) {
	notifyCh := make(chan struct{})

	api := NewApiServer(":9999", "/")
	api.HandleFunc(http.MethodGet, "/test", func(c *ApiCtx) {
		time.Sleep(100 * time.Millisecond)
		notifyCh <- struct{}{}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go api.Serve(ctx)

	numConcurrent := 100
	for i := 0; i < numConcurrent; i++ {
		go func() {
			req, err := http.NewRequest(http.MethodGet, "http://localhost:9999/test", nil)
			if err != nil {
				panic(err)
			}
			if _, err := http.DefaultClient.Do(req); err != nil {
				panic(err)
			}
		}()
	}

	timer := time.NewTimer(200 * time.Millisecond)

	total := 0
	select {
	case <-timer.C:
		t.Fatal("should have completed before timer")
	case <-notifyCh:
		total++
		if total >= numConcurrent {
			return
		}
	}
}
