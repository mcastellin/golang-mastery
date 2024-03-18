package objcache

// ItemGetterFn type is the signature of the function that can be used
// by the GetCachedResource wrapper to fetch information if missing
// from the cache.
type ItemGetterFn func(string) (any, error)

// GetCachedResource is a utility function that either returns items by key from the cache, or
// fetch the item using the ItemGetterFn if missing.
//
// Note that this function also caches nil responses!
func GetCachedResource(c *ObjectsCache, key string, f ItemGetterFn) (*CacheItem, error) {
	item := c.Get(key)
	if item == nil {
		v, err := f(key)
		if err != nil {
			return nil, err
		}

		item = c.Put(key, v)
	}

	return item, nil
}
