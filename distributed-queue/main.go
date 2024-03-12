package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/db"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/prefetch"
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

	mgr := &db.ShardManager{}
	for _, c := range shardConfs {
		_, err := mgr.Add(c.Id, c.Master, c.ConnString)
		if err != nil {
			panic(err)
		}
	}
	defer mgr.Close()

	prefetchBuf := &prefetch.PriorityBuffer{}
	prefetchBuf.Serve()

	msgRepo := &db.MessageRepository{}
	// TODO we should have one wAckNack worker per shard and put a router in
	// front to forward the request to the right worker
	ackNackBuf := make(chan AckNackRequest, 1000)
	wAckNack := &AckNackWorker{ShardMgr: mgr, MsgRepository: msgRepo, Buffer: ackNackBuf}
	go wAckNack.Run()
	defer wAckNack.Stop()

	for _, shard := range mgr.Shards() {
		wEnqueue := &EnqueueWorker{Shard: shard, MsgRepository: msgRepo, Buffer: buf}
		wDequeue := &DequeueWorker{Shard: shard, MsgRepository: msgRepo, PrefetchBuffer: prefetchBuf}
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
		NamespaceRepo: &db.NamespaceRepository{},
		EnqueueBuffer: buf,
		DequeueBuffer: prefetchBuf,
		AckNackBuffer: ackNackBuf,
	}
	api := &APIService{Handler: hh}
	if err := api.Serve(ctx); err != nil {
		panic(err)
	}
}
