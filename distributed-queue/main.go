package main

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/db"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/prefetch"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/queue"
	"go.uber.org/zap"
)

// TODO
// shard configuration is fixed for now. Once I implement scale-out logic to dynamically add
// and remove shards to ShardManager db connections be auto-discovered by the system.
var shardConfs = []struct {
	Id         uint32
	Main       bool
	ConnString string
}{
	{uint32(10), true, "postgres://user:changeme@localhost:5431/foqs?sslmode=disable"},
	{uint32(20), false, "postgres://user:changeme@localhost:5432/foqs?sslmode=disable"},
	{uint32(30), false, "postgres://user:changeme@localhost:5433/foqs?sslmode=disable"},
	{uint32(40), false, "postgres://user:changeme@localhost:5434/foqs?sslmode=disable"},
}

const defaultBufferSize = 500

type httpServer interface {
	Serve(context.Context) error
}

type workerStarterStopper interface {
	Run() error
	Stop() error
}

type App struct {
	logger  *zap.Logger
	server  httpServer
	workers []workerStarterStopper
	cleanup func()
}

func (a *App) AddWorker(w workerStarterStopper) {
	a.workers = append(a.workers, w)
}

func (a *App) SetCleanupFn(cleanup func()) {
	a.cleanup = cleanup
}

func (a *App) Run() error {
	for _, w := range a.workers {
		if err := w.Run(); err != nil {
			return err
		}
		defer w.Stop()
	}

	if a.cleanup != nil {
		defer a.cleanup()
	}

	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	return a.server.Serve(ctx)
}

func createApp(bindAddr string, logger *zap.Logger) *App {
	app := &App{logger: logger}

	mgr := &db.ShardManager{}
	for _, c := range shardConfs {
		_, err := mgr.Add(c.Id, c.Main, c.ConnString)
		if err != nil {
			panic(err)
		}
	}
	app.SetCleanupFn(func() {
		defer mgr.Close()
	})

	enqueueBuffer := make(chan queue.EnqueueRequest, defaultBufferSize)

	prefetchBuf := prefetch.NewPriorityBuffer()
	app.AddWorker(prefetchBuf)

	ackNackRouter := &queue.AckNackRouter{}

	for _, shard := range mgr.Shards() {
		app.AddWorker(queue.NewEnqueueWorker(shard, enqueueBuffer))
		app.AddWorker(queue.NewDequeueWorker(shard, prefetchBuf))

		ackNackBuf := make(chan queue.AckNackRequest, defaultBufferSize)
		ackNackW := queue.NewAckNackWorker(shard, ackNackBuf)

		app.AddWorker(ackNackW)
		ackNackRouter.RegisterWorker(shard.Id, ackNackW)
	}

	nsService := &NamespaceService{
		MainShard:    mgr.MainShard(),
		NsRepository: &db.NamespaceRepository{},
	}
	msgService := &MessagesService{
		EnqueueBuffer: enqueueBuffer,
		DequeueBuffer: prefetchBuf,
		AckNackRouter: ackNackRouter,
	}

	api := NewApiServer(bindAddr, "/")
	api.HandleFunc(http.MethodGet, "/ns", nsService.HandleGetNamespaces)
	api.HandleFunc(http.MethodPost, "/ns", nsService.HandleCreateNamespace)
	api.HandleFunc(http.MethodPost, "/message/enqueue", msgService.HandleEnqueue)
	api.HandleFunc(http.MethodPost, "/message/dequeue", msgService.HandleDequeue)
	api.HandleFunc(http.MethodPost, "/message/ack", msgService.HandleAckNack)
	app.server = api

	return app
}

func main() {

	logger := zap.Must(zap.NewProduction())
	defer logger.Sync()
	logger.Info("application starting: distributed-queue")

	addr := os.Getenv("BIND_ADDR")
	if len(addr) == 0 {
		addr = ":8080"
	}

	app := createApp(addr, logger)

	if err := app.Run(); err != nil {
		panic(err)
	}
}
