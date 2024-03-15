package objcache

import (
	"fmt"
	"testing"
	"time"
)

func getKey(n int) string {
	return fmt.Sprintf("key-%d", n)
}

type mockItem struct {
	Payload int
}

func TestCacheOperations(t *testing.T) {

	maxItems := 10
	numItems := 10000
	cache := NewObjectsCache(maxItems, time.Second)

	for i := 0; i < numItems; i++ {
		cache.Put(getKey(i), mockItem{i})
	}

	if len(cache.items) != maxItems {
		t.Fatalf("cache exceeded the maximum allowed size: found %d", len(cache.items))
	}

	fmt.Println(cache.items)

	n := numItems - 3
	item := cache.Get(getKey(n))
	if item == nil {
		t.Fatal("returned nil item")
	}

	if item.Value.(mockItem).Payload != n {
		t.Fatalf("wrong key returned: expected %d, found %d", n, item.Value.(mockItem).Payload)
	}

	cache.Delete(getKey(n))
	cache.Delete(getKey(n + 1))
	cache.Delete(getKey(n + 2))

	item = cache.Get(getKey(n))
	if item != nil {
		t.Fatal("item was not deleted from cache.")
	}

	if len(cache.evictionHeap) != len(cache.items) {
		t.Fatal("sync between objects store and eviction heap was not maintained")
	}
}

func TestEmptyStore(t *testing.T) {

	maxItems := 10
	numItems := 10000
	cache := NewObjectsCache(maxItems, time.Second)

	for i := 0; i < numItems; i++ {
		cache.Put(getKey(i), mockItem{i})
	}

	for i := 0; i < numItems; i++ {
		cache.Delete(getKey(i))
	}

	if len(cache.evictionHeap) != len(cache.items) {
		t.Fatal("sync between objects store and eviction heap was not maintained")
	}

}
