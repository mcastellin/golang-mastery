package main

import "time"

type backoffType int

const (
	backoffLog backoffType = 0
)

type backoffStrategy struct {
	Type backoffType

	backoffDuration time.Duration
}

func (s *backoffStrategy) next() <-chan time.Time {
	return time.After(s.backoffDuration) //??
}

func (s *backoffStrategy) backoff() {
	switch s.Type {
	case backoffLog:
		s.backoffDuration = time.Second
	default:
		s.backoffDuration = time.Second
	}
}
