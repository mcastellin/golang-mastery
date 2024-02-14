package gossip

import "math/rand"

// randIndexes is an internal function to generate random index values
// that can be used to build randomized lists of items.
// The items parameter represents the number of items to randomize from,
// and generate parameter represents requested number of random indexes.
//
// In case the number of items is smaller than the requested
// generated items, this function will return only up-to the number of
// available items.
func randIndexes(items int, generate int) []int {

	num := generate
	if generate > items {
		num = items

	}
	randIdxs := make([]int, num)

	for i := 0; i < num; i++ {
		idx := rand.Intn(num)
		randIdxs[i] = idx
	}

	return randIdxs
}
