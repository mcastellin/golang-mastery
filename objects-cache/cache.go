package objcache

import (
	"container/heap"
	"sync"
	"time"
)

// CacheItem represent the structure of an item we can store in the ObjectsCache
type CacheItem struct {
	Key        string
	Value      any
	ExpiryTime time.Time
}

// NewObjectsCache creates a new ObjectsCache instance
func NewObjectsCache(maxItems int, ttl time.Duration) *ObjectsCache {
	itemsEvictionHeap := make(cacheItemHeap, 0)
	heap.Init(&itemsEvictionHeap)

	return &ObjectsCache{
		maxItems:     maxItems,
		itemsTTL:     ttl,
		items:        map[string]*CacheItem{},
		evictionHeap: itemsEvictionHeap,
	}
}

// ObjectsCache is used to store any object in-memory for fast retrieval.
type ObjectsCache struct {
	maxItems int
	itemsTTL time.Duration

	items        map[string]*CacheItem
	evictionHeap cacheItemHeap
	mu           sync.RWMutex
}

// Put a new item into the ObjectsCache
func (c *ObjectsCache) Put(k string, v any) *CacheItem {
	c.Delete(k)

	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.items) >= c.maxItems {
		c.evict(1)
	}
	item := &CacheItem{
		Key:        k,
		Value:      v,
		ExpiryTime: time.Now().Add(c.itemsTTL),
	}
	c.items[k] = item
	heap.Push(&c.evictionHeap, item)

	return item
}

func (c *ObjectsCache) evict(n int) {
	for i := 0; i < n && len(c.evictionHeap) > 0; i++ {
		evicted := heap.Pop(&c.evictionHeap)
		delete(c.items, evicted.(*CacheItem).Key)
	}
}

// Delete an item from the cache
func (c *ObjectsCache) Delete(k string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.items, k)
	for i := 0; i < len(c.evictionHeap); i++ {
		if c.evictionHeap[i].Key == k {
			heap.Remove(&c.evictionHeap, i)
			return
		}
	}
}

// Get an item from the cache. If we're past the item's expiryTime
// return nil.
func (c *ObjectsCache) Get(k string) *CacheItem {
	c.mu.RLock()
	item, ok := c.items[k]
	c.mu.RUnlock()
	if !ok {
		return nil
	}

	if time.Now().After(item.ExpiryTime) {
		return nil
	}
	return item
}

// cacheItemHeap implements the heap.Interface
type cacheItemHeap []*CacheItem

func (h cacheItemHeap) Len() int {
	return len(h)
}

func (h cacheItemHeap) Less(i, j int) bool {
	return h[i].ExpiryTime.Before(h[j].ExpiryTime)
}

func (h cacheItemHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *cacheItemHeap) Push(v any) {
	item := v.(*CacheItem)
	*h = append(*h, item)
}

func (h *cacheItemHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}
