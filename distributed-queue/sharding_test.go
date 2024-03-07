package main

import (
	"fmt"
	"testing"
)

func TestGetShardById(t *testing.T) {
	mgr := &ShardManager{}
	defer mgr.Close()

	shard := mgr.Get(1234)
	fmt.Println(shard)
	t.Fatal()
}
