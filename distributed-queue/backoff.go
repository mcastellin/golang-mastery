package main

import (
	"time"
)

const (
	defaultBackoffBase = 10 * time.Millisecond
	maxBackoffExponent = 5
)

func newBackoff() *backoffStrategy {
	return &backoffStrategy{
		initialDuration: defaultBackoffBase,
		factor:          2,
		durationCap:     5 * time.Second,
	}
}

type backoffStrategy struct {
	initialDuration time.Duration
	factor          float32
	durationCap     time.Duration

	duration       time.Duration
	nextActivation time.Time
}

func (s *backoffStrategy) backoff() {
	s.duration = s.initialDuration + time.Duration(float32(s.duration)*s.factor)
	if s.duration > s.durationCap {
		s.duration = s.durationCap
	}
	s.nextActivation = time.Now().Add(s.duration)
}

func (s *backoffStrategy) active() bool {
	return time.Now().Before(s.nextActivation)
}

func (s *backoffStrategy) after() <-chan time.Time {
	return time.After(s.duration)
}

func (s *backoffStrategy) reset() {
	s.duration = 0
	s.nextActivation = time.Now()
}
