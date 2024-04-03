package main

import (
	"context"
	"fmt"
	"time"
)

// [cwl:b blockingOp]

// mockComplexOp represents a long-running, uninterruptible operation
// like a complex computation, a database transaction or an HTTP request
type mockComplexOp struct {
	Duration time.Duration
	timer    *time.Timer
}

// Do performs the uninterruptible operation.
// This mock implementation just sleeps for a set Duration
func (op *mockComplexOp) Do() error {
	op.timer = time.NewTimer(op.Duration)
	// reading from timer's channel is uninterruptible
	<-op.timer.C
	return nil
}

// Stop will gracefully terminate the long-running operation
func (op *mockComplexOp) Stop() {
	if op.timer != nil {
		if !op.timer.Stop() {
			<-op.timer.C
		}
	}
}

// [/cwl:b]

type longRunningOp interface {
	Do() error
	Stop()
}

// [cwl:b runOnce]

// runOpOnce is just an example of how to execute a long-running operation in the
// background without any option for task cancellation.
func runOpOnce(op longRunningOp, completedCh chan struct{}) {
	op.Do()
	close(completedCh)
}

// [/cwl:b]

// runOpWithCancelCh executes the long-running operation with the option for
// graceful termination using a cancelCh channel.
//
// Note that all signal channels are of type `chan struct{}` so the empty struct
// won't allocate any memory.
// Another important aspect of using signal channels is that rather than reading/writing
// signals to the chan we use the `close(chan)`. The main benefit of using close instead of
// communicating signal codes is that writing to a channel could block program execution if
// the channel is unbuffered or full and there no other process is actively consuming it.
// Closing the channel is a safer option in this regard and has the side benefit of affecting
// multiple routines blocked on channel read using a single operation.
//
// [cwl:b cancelWithChannel]
func runOpWithCancelCh(
	op longRunningOp,
	completedCh chan struct{},
	cancelCh chan struct{}) {

	// decouple uninterruptible operation from its wrapper
	// by running in a new sub-routine, allowing this function
	// to handle cancellation logic.
	innerCompletedCh := make(chan struct{})
	go func() {
		op.Do()
		close(innerCompletedCh)
	}()

	select {
	case <-innerCompletedCh:
		// normal program execution, background process completed
		// successfully.

	case <-cancelCh:
		// received cancellation signal before operation could
		// complete. Requesting termination.
		op.Stop()
	}

	// always sending completion signal to avoid blocking callers
	close(completedCh)
}

// [/cwl:b]

// [cwl:b cancelWithContext]

// runOpWithContext executes the long-running operation and handle cancellation
// when the Context is Done.
func runOpWithContext(
	ctx context.Context,
	op longRunningOp,
	completed chan struct{}) {

	// decouple uninterruptible operation from its wrapper
	fnCompleted := make(chan struct{})
	go func() {
		op.Do()
		close(fnCompleted)
	}()

	select {
	case <-fnCompleted:
		// normal program execution, background process completed
		// successfully.

	case <-ctx.Done():
		// Context timed-out or cancelled before operation could
		// complete. Requesting termination.
		op.Stop()
	}

	// always sending completion signal to avoid blocking callers
	close(completed)
}

// [/cwl:b]

func main() {
	fmt.Println("Usage: Run with `go test ./... -v`")
}
