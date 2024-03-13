package wait

import (
	"testing"
	"time"
)

func TestBackoffInitialState(t *testing.T) {
	bo := NewBackoff(time.Second, 2, time.Minute)

	select {
	case <-time.After(time.Millisecond):
		t.Fatal("backoff should not have blocked execution")
	case <-bo.After():
		// if no backoff applied, should immediately return
		return
	}
}

func TestBackoff(t *testing.T) {
	bo := NewBackoff(time.Second, 2, time.Minute)
	bo.Backoff()

	select {
	case <-time.After(time.Millisecond):
		return
	case <-bo.After():
		t.Fatal("backoff should delayed execution")
	}
}
