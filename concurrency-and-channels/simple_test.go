package main

import (
	"testing"
)

func TestSimpleIntegral(t *testing.T) {
	numWorkers := 5
	result := SquareSum(1000, numWorkers)

	var expected int64 = 332833500
	if result != expected {
		t.Fatalf("calculated integral is wrong: expected %d, found: %d", expected, result)
	}
}
