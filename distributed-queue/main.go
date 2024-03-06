package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/lib/pq"
)

var connStrings = []string{
	"postgres://user:changeme@localhost:5431/foqs?sslmode=disable",
	"postgres://user:changeme@localhost:5432/foqs?sslmode=disable",
	"postgres://user:changeme@localhost:5433/foqs?sslmode=disable",
	"postgres://user:changeme@localhost:5434/foqs?sslmode=disable",
}

func main() {

	ctx, cancel := signal.NotifyContext(context.Background(),
		os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	buf := make(chan EnqueueRequest, 100)

	mgr := &ShardManager{}
	for i, c := range connStrings {
		meta, err := mgr.Add(fmt.Sprintf("Shard%d", i), c)
		if err != nil {
			panic(err)
		}
		w := &EnqueueWorker{Shard: meta, Buffer: buf}
		go w.Run()
		defer w.Stop()
	}

	defer mgr.Close()

	var err error
	var msg Message
	err = msg.CreateTable(mgr)
	if err != nil {
		panic(err)
	}

	var queue Queue
	err = queue.CreateTable(mgr)
	if err != nil {
		panic(err)
	}

	meta, err := mgr.Connect(nil)
	if err != nil {
		panic(err)
	}

	rows, err := meta.Conn().Query("SELECT version();")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	for rows.Next() {
		var version string
		rows.Scan(&version)
		fmt.Println("Version", version)
	}

	hh := &Handler{ShardMgr: mgr, EnqueueBuffer: buf}
	api := &APIService{Handler: hh}
	if err := api.Serve(ctx); err != nil {
		panic(err)
	}
}
