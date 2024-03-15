package main

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest"
)

func TestApiServerConcurrency(t *testing.T) {
	notifyCh := make(chan struct{})
	logger := zaptest.NewLogger(t, zaptest.Level(zap.WarnLevel))

	port := bindAvailablePort(t)
	bindAddr := fmt.Sprintf(":%d", port)
	api := NewApiServer(bindAddr, "/", logger)
	api.HandleFunc(http.MethodGet, "/test", func(c *ApiCtx) {
		time.Sleep(100 * time.Millisecond)
		notifyCh <- struct{}{}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	notify := make(chan struct{})
	go api.Serve(ctx, notify)
	<-notify

	numConcurrent := 100
	for i := 0; i < numConcurrent; i++ {
		go func() {
			url := fmt.Sprintf("http://localhost:%d/test", port)
			req, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				panic(err)
			}
			cli := http.Client{Timeout: time.Second}
			if _, err := cli.Do(req); err != nil {
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

func TestApiServerBaseUrl(t *testing.T) {
	logger := zaptest.NewLogger(t, zaptest.Level(zap.WarnLevel))

	port := bindAvailablePort(t)
	bindAddr := fmt.Sprintf(":%d", port)
	api := NewApiServer(bindAddr, "/base", logger)
	api.HandleFunc(http.MethodGet, "/test/path", func(c *ApiCtx) {
		c.JsonResponse(http.StatusOK, H{})
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	notify := make(chan struct{})
	go api.Serve(ctx, notify)
	<-notify

	url := fmt.Sprintf("http://localhost:%d/base/test/path", port)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}

	cli := http.Client{Timeout: time.Second}
	resp, err := cli.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	if resp.StatusCode != 200 {
		t.Fatalf("returned status code %d, expected %d", resp.StatusCode, 200)
	}
}

func bindAvailablePort(t testing.TB) int {
	return 9999
}
