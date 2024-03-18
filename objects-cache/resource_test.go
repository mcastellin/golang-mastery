package objcache

import (
	"fmt"
	"testing"
	"time"
)

func mockResourceGetter(key string) string {
	time.Sleep(100 * time.Millisecond)
	return fmt.Sprintf("%s-value", key)
}

func TestGetCachedResource(t *testing.T) {
	c := NewObjectsCache(10, time.Second)

	getFn := func(k string) (any, error) {
		return mockResourceGetter(k), nil
	}

	item, err := GetCachedResource(c, "test", getFn)
	if err != nil {
		t.Fatal(err)
	}

	if item.Value.(string) != "test-value" {
		t.Fatalf("wrong value returned from the cache: %s", item.Value.(string))
	}

	start := time.Now()
	for i := 0; i < 100; i++ {
		if _, err := GetCachedResource(c, "test", getFn); err != nil {
			t.Fatal(err)
		}
	}
	if time.Since(start) > 150*time.Millisecond {
		t.Fatal("operation took too long to complete")
	}
}
