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

const defaultBufferSize int = 500

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

type httpServer interface {
	Serve(context.Context) error
}

type workerStarterStopper interface {
	Run()
	Stop() error
}

type App struct {
	server  httpServer
	workers []workerStarterStopper
}

func (a *App) AddWorker(w workerStarterStopper) {
	a.workers = append(a.workers, w)
}

func (a *App) Run() error {

	for _, w := range a.workers {
		go w.Run()
		defer w.Stop()
	}

	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	return a.server.Serve(ctx)
}

func main() {
	app := &App{}

	mgr := &db.ShardManager{}
	for _, c := range shardConfs {
		_, err := mgr.Add(c.Id, c.Master, c.ConnString)
		if err != nil {
			panic(err)
		}
	}
	defer mgr.Close()

	enqueueBuf := make(chan EnqueueRequest, defaultBufferSize)
	ackNackBuf := make(chan AckNackRequest, defaultBufferSize)

	msgRepo := &db.MessageRepository{}

	prefetchBuf := &prefetch.PriorityBuffer{}
	app.AddWorker(prefetchBuf)
	// TODO we should have one wAckNack worker per shard and put a router in
	// front to forward the request to the right worker
	app.AddWorker(&AckNackWorker{ShardMgr: mgr, MsgRepository: msgRepo, Buffer: ackNackBuf})

	for _, shard := range mgr.Shards() {
		app.AddWorker(&EnqueueWorker{Shard: shard, MsgRepository: msgRepo, Buffer: enqueueBuf})
		app.AddWorker(&DequeueWorker{Shard: shard, MsgRepository: msgRepo, PrefetchBuffer: prefetchBuf})
	}

	var version string
	err := mgr.Master().Conn().QueryRow("SELECT version()").Scan(&version)
	if err != nil {
		panic(err)
	}
	fmt.Println("PostgreSQL version:", version)

	hh := &Handler{
		ShardMgr:      mgr,
		MainShard:     mgr.Master(),
		NamespaceRepo: &db.NamespaceRepository{},
		EnqueueBuffer: enqueueBuf,
		DequeueBuffer: prefetchBuf,
		AckNackBuffer: ackNackBuf,
	}
	app.server = &APIService{Handler: hh}

	if err := app.Run(); err != nil {
		panic(err)
	}
}
