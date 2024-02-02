package main

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// A more realistic example of long-running background operation. The webServerOp
// wraps an http.Server and runs it in the background. It supports shutdown using
// context cancellation.
type webServerOp struct {
	Server             *http.Server
	ForceShutdownAfter time.Duration
}

// Runs ListenAndServer for the configured http.Server
func (op *webServerOp) Do() error {
	return op.Server.ListenAndServe()
}

// Handles graceful http.Server shutdown
//
// If the server is not shutdown within the defined ForceShutdownAfter time period
// it is forcefully shutdown
func (op *webServerOp) Stop() {
	if op.ForceShutdownAfter == 0 {
		op.ForceShutdownAfter = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), op.ForceShutdownAfter)
	defer cancel()
	if err := op.Server.Shutdown(ctx); err != nil {
		fmt.Println("http server forcefully shutdown")
	}
	fmt.Println("graceful HTTP server shutdown completed")
}
