package main

import (
	"context"
	"testing"
	"time"
)

func TestBackgroundTask(t *testing.T) {
	op := &mockComplexOp{Duration: time.Second}

	completedCh := make(chan struct{})
	go runOpOnce(op, completedCh)

	select {
	case <-completedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("task did not complete. Operation timed out")
	}
}

func TestBackgroundTaskOnceToCompletion(t *testing.T) {
	op := &mockComplexOp{Duration: time.Second}

	completedCh := make(chan struct{})
	cancelCh := make(chan struct{})
	go runOpWithCancelCh(op, completedCh, cancelCh)

	select {
	case <-completedCh:
		// execution should complete before the 2 seconds timeout
	case <-time.After(2 * time.Second):
		t.Fatal("task execution did not finish")
	}
	close(cancelCh)
}

// [cwl:b testRunWithCancelCh]
func TestBackgroundTaskWithChanCancel(t *testing.T) {
	// long-running operation will block for 5 seconds
	op := &mockComplexOp{Duration: 5 * time.Second}

	completedCh := make(chan struct{})
	cancelCh := make(chan struct{})
	go runOpWithCancelCh(op, completedCh, cancelCh)

	// sleep 1 second and close cancelCh to request
	// cancellation. The operation should cancel immediately.
	time.Sleep(time.Second)
	close(cancelCh)

	select {
	case <-completedCh:
		// task execution should cancel immediately and
		// signal goroutine completion

	case <-time.After(time.Second):
		t.Fatal("task execution was not cancelled timely")
	}
} // [/cwl:b]

// [cwl:b testRunWithContextCancel]
func TestBackgroundTaskWithContext(t *testing.T) {
	// long-running operation will block for 5 seconds
	op := &mockComplexOp{Duration: 5 * time.Second}

	// setup context that times-out in 1 second.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	completedCh := make(chan struct{})
	go runOpWithContext(ctx, op, completedCh)

	select {
	case <-completedCh:
		// task execution should cancel immediately and
		// signal goroutine completion

	case <-time.After(2 * time.Second):
		t.Fatal("task execution was not cancelled timely")
	}
} // [/cwl:b]
