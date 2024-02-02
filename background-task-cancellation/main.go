package main

import (
	"context"
	"fmt"
	"time"
)

// An example type that represents a long-running uninterruptible operation
// like a complex computation, a database transation or HTTP interaction
type mockComplexOp struct {
	Duration time.Duration
	timer    *time.Timer
}

// Perform the long-running operation.
// This mock implementation just sleeps for a set Duration
func (op *mockComplexOp) Do() error {
	op.timer = time.NewTimer(op.Duration)

	<-op.timer.C
	return nil
}

// Gracefully stops the long-running operation
func (op *mockComplexOp) Stop() {
	if op.timer != nil {
		if !op.timer.Stop() {
			<-op.timer.C
		}
	}
}

type longRunningOp interface {
	Do() error
	Stop()
}

// This function is just an example of how to execute a long running operation
// in the background without cancellation option
func runOpOnce(op longRunningOp, completed chan struct{}) {
	op.Do()
	close(completed)
}

// This function executes the long-running operation with the option for
// cancellation using a cancel channel.
//
// Note that all signal channels are of type `chan struct{}` so the empty struct
// won't allocate any memory.
// Another important aspect of using signal channels is that rather than writing data
// to the chan we use the `close(chan)`. Sending data to an unbuffered chan is a blocking
// operation, as opposed to closing the channel which doesn't block program execution
func runOpWithCancelCh(op longRunningOp, completed chan struct{}, cancel chan struct{}) {
	defer close(completed)

	fnCompleted := make(chan struct{})
	go func() {
		op.Do()
		close(fnCompleted)
	}()

	select {
	case <-cancel:
		op.Stop()
	case <-fnCompleted:
	}
}

// This function executes the long-running operation using context to handle cancellation.
//
// With context the cancellation logic is not necessarily simpler. It provides a nicer
// interface for the caller, which can wrap parent contexts and set timeouts and deadlines.
func runOpWithContext(ctx context.Context, op longRunningOp, completed chan struct{}) {
	defer close(completed)
	fnCompleted := make(chan struct{})
	go func() {
		op.Do()
		close(fnCompleted)
	}()

	select {
	case <-ctx.Done():
		op.Stop()
	case <-fnCompleted:
	}
}

func main() {
	fmt.Println("Usage: Run with `go test ./... -v`")
}
