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

func TestBackoffMaxBound(t *testing.T) {
	bo := NewBackoff(time.Second, 2, time.Minute)
	for i := 0; i < 10; i++ {
		bo.Backoff()
	}

	if bo.duration != time.Minute {
		t.Fatalf("backoff duration escaped max bound: found %s", bo.duration.String())
	}
}
