package main

import (
	"context"
	"testing"
	"time"
)

func TestBackgroundTask(t *testing.T) {
	op := &mockComplexOp{Duration: time.Second}

	respCh := make(chan struct{})
	go runOpOnce(op, respCh)

	select {
	case <-respCh:
	case <-time.After(5 * time.Second):
		t.Fatal("task did not complete. Operation timed out")
	}
}

func TestBackgroundTaskOnceToCompletion(t *testing.T) {
	op := &mockComplexOp{Duration: time.Second}

	respCh := make(chan struct{})
	cancelCh := make(chan struct{})
	go runOpWithCancelCh(op, respCh, cancelCh)

	select {
	case <-respCh:
		// execution should complete before the 2 seconds timeout
	case <-time.After(2 * time.Second):
		t.Fatal("task execution did not finish")
	}
	close(cancelCh)
}

func TestBackgroundTaskOnceWithCancellation(t *testing.T) {
	op := &mockComplexOp{Duration: 5 * time.Second}

	respCh := make(chan struct{})
	cancelCh := make(chan struct{})
	go runOpWithCancelCh(op, respCh, cancelCh)

	<-time.After(time.Second)
	close(cancelCh) // task should cancel immediately

	select {
	case <-respCh:
		// execution should have been cancelled
	case <-time.After(2 * time.Second):
		t.Fatal("task execution was not cancelled")
	}
}

func TestBackgroundTaskWithContext(t *testing.T) {
	op := &mockComplexOp{Duration: 5 * time.Second}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	respCh := make(chan struct{})
	go runOpWithContext(ctx, op, respCh)

	select {
	case <-respCh:
		// execution should timeout after 1 second
	case <-time.After(2 * time.Second):
		t.Fatal("task execution was not cancelled")
	}
}
