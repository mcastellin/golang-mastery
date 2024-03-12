package wait

import (
	"time"
)

// NewBackoff creates a new instance of a BackoffStrategy.
func NewBackoff(base time.Duration, factor float32, backoffCap time.Duration) *BackoffStrategy {
	return &BackoffStrategy{
		initialDuration: base,
		factor:          factor,
		durationCap:     backoffCap,
	}
}

type BackoffStrategy struct {
	initialDuration time.Duration
	factor          float32
	durationCap     time.Duration

	duration       time.Duration
	nextActivation time.Time
}

func (s *BackoffStrategy) Backoff() {
	s.duration = s.initialDuration + time.Duration(float32(s.duration)*s.factor)
	if s.duration > s.durationCap {
		s.duration = s.durationCap
	}
	s.nextActivation = time.Now().Add(s.duration)
}

// Active returns true if the backoff timeout is expired and it's ok
// to proceed with the operation
func (s *BackoffStrategy) Active() bool {
	return time.Now().After(s.nextActivation)
}

// After returns a channel that notifies when it's ok to proceed
func (s *BackoffStrategy) After() <-chan time.Time {
	return time.After(s.duration)
}

// Reset the backoff strategy to its initial values
func (s *BackoffStrategy) Reset() {
	s.duration = 0
	s.nextActivation = time.Now()
}
