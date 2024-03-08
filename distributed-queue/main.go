package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"
)

var shardConfs = []struct {
	Id         uint32
	Master     bool
	ConnString string
}{
	{uint32(10), true, "postgres://user:changeme@localhost:5431/foqs?sslmode=disable"},
	{uint32(20), false, "postgres://user:changeme@localhost:5432/foqs?sslmode=disable"},
	{uint32(30), false, "postgres://user:changeme@localhost:5433/foqs?sslmode=disable"},
	{uint32(40), false, "postgres://user:changeme@localhost:5434/foqs?sslmode=disable"},
}

func main() {

	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	buf := make(chan EnqueueRequest, 500)

	mgr := &ShardManager{}
	for _, c := range shardConfs {
		_, err := mgr.Add(c.Id, c.Master, c.ConnString)
		if err != nil {
			panic(err)
		}
	}
	defer mgr.Close()

	prefetchBuf := &PriorityBuffer{}
	prefetchBuf.Serve()
	for _, shard := range mgr.Shards() {
		wEnqueue := &EnqueueWorker{Shard: shard, Buffer: buf}
		wDequeue := &DequeueWorker{Shard: shard, PrefetchBuffer: prefetchBuf}
		go wEnqueue.Run()
		go wDequeue.Run()
		defer wEnqueue.Stop()
		defer wDequeue.Stop()
	}

	var version string
	err := mgr.Master().Conn().QueryRow("SELECT version();").Scan(&version)
	if err != nil {
		panic(err)
	}
	fmt.Println("Version", version)

	hh := &Handler{
		ShardMgr:      mgr,
		MainShard:     mgr.Master(),
		EnqueueBuffer: buf,
		DequeueBuffer: prefetchBuf,
	}
	api := &APIService{Handler: hh}
	if err := api.Serve(ctx); err != nil {
		panic(err)
	}
}
