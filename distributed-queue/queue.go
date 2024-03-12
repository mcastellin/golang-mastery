package main

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/db"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/domain"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/prefetch"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/wait"
)

const (
	dequeueBatchSize       = 100
	backoffInitialDuration = 10 * time.Millisecond
	backoffMaxDuration     = 5 * time.Second
	backoffFactor          = 2
)

type messageSaver interface {
	Save(*db.ShardMeta, *domain.Message) error
}
type messageAckNacker interface {
	Ack(*db.ShardMeta, domain.UUID, bool) error
}
type messageSearcherUpdater interface {
	FindMessagesReadyForDelivery(*db.ShardMeta, bool, []string,
		int, ...db.OptsFn) ([]domain.Message, error)

	UpdatePrefetchedBatch(*db.ShardMeta, []domain.UUID, bool) (*sql.Tx, error)
}

type EnqueueResponse struct {
	MsgId domain.UUID
	Err   error
}

type EnqueueRequest struct {
	Msg    domain.Message
	RespCh chan<- EnqueueResponse
}

type EnqueueWorker struct {
	Shard         *db.ShardMeta
	MsgRepository messageSaver

	Buffer chan EnqueueRequest

	shutdown chan chan error
}

func (w *EnqueueWorker) Run() {
	w.shutdown = make(chan chan error)
	cleanup := func() {
		close(w.shutdown)
	}
	defer cleanup()

	for {
		select {
		case respCh := <-w.shutdown:
			respCh <- nil
			return

		case enqReq := <-w.Buffer:
			msg := enqReq.Msg
			var reply EnqueueResponse
			if err := w.MsgRepository.Save(w.Shard, &msg); err != nil {
				reply.Err = err
				enqReq.RespCh <- reply
				continue
			}
			reply.MsgId = msg.Id
			enqReq.RespCh <- reply
		}
	}
}

func (w *EnqueueWorker) Stop() error {
	errCh := make(chan error)
	w.shutdown <- errCh

	return <-errCh
}

type DequeueWorker struct {
	Shard         *db.ShardMeta
	MsgRepository messageSearcherUpdater

	PrefetchBuffer *prefetch.PriorityBuffer

	shutdown      chan chan error
	topicBackoffs map[string]*wait.BackoffStrategy
}

func (w *DequeueWorker) Run() {
	w.shutdown = make(chan chan error)
	cleanup := func() {
		close(w.shutdown)
	}
	defer cleanup()

	w.topicBackoffs = map[string]*wait.BackoffStrategy{}
	retrieveBackoff := wait.NewBackoff(backoffInitialDuration, backoffFactor, backoffMaxDuration)
	for {
		select {
		case respCh := <-w.shutdown:
			respCh <- nil
			return
		case <-retrieveBackoff.After():
			//TODO implement backoff strategy for topics.
			// This could be achieved by storing which topics we're asked to backoff and exclude
			// those topics from the search for a certain amount of time
			// Also, to reduce the pressure on the database we could throttle requests and
			// implement some internal backoff strategy if requests came back empty
			// Implementing this worker comes with its own sets of challenges if we want to avoid
			// overwhelming the database
			exclusions := excludedTopics(w.topicBackoffs)
			msgs, err := w.MsgRepository.FindMessagesReadyForDelivery(w.Shard, false,
				exclusions, prefetch.MaxPrefetchItemCount, db.WithLimit(dequeueBatchSize))
			if err != nil {
				fmt.Println(err)
				continue
			}

			if len(msgs) == 0 {
				retrieveBackoff.Backoff()
				continue
			}
			retrieveBackoff.Reset()

			replyCh := make(chan []prefetch.PrefetchStatusCode)
			envelope := prefetch.IngestEnvelope{Batch: msgs, RespCh: replyCh}
			w.PrefetchBuffer.C() <- envelope

			reply := <-replyCh
			close(replyCh)

			ids := make([]domain.UUID, 0)
			for i, r := range reply {
				switch r {
				case prefetch.PrefetchStatusOk:
					ids = append(ids, msgs[i].Id)

				case prefetch.PrefetchStatusBackoff:
					b := w.topicBackoffs[msgs[i].Topic]
					if b == nil {
						b = wait.NewBackoff(backoffInitialDuration, backoffFactor, backoffMaxDuration)
						w.topicBackoffs[msgs[i].Topic] = b
					}
					b.Backoff()
				}
			}
			tx, err := w.MsgRepository.UpdatePrefetchedBatch(w.Shard, ids, true)
			if err != nil {
				fmt.Println(err)
				continue
			}
			tx.Commit()
		}
	}
}

func excludedTopics(backoffs map[string]*wait.BackoffStrategy) []string {
	excludes := []string{}
	for t, b := range backoffs {
		if !b.Active() {
			excludes = append(excludes, t)
		} else {
			delete(backoffs, t)
		}
	}
	return excludes
}

func (w *DequeueWorker) Stop() error {
	errCh := make(chan error)
	w.shutdown <- errCh

	return <-errCh
}

type AckNackRequest struct {
	Id  domain.UUID
	Ack bool
}

type AckNackWorker struct {
	ShardMgr      *db.ShardManager
	MsgRepository messageAckNacker

	Buffer chan AckNackRequest

	shutdown chan chan error
}

func (w *AckNackWorker) Run() {
	w.shutdown = make(chan chan error)
	cleanup := func() {
		close(w.shutdown)
	}
	defer cleanup()

	for {
		select {
		case respCh := <-w.shutdown:
			respCh <- nil
			return

		case ackNack := <-w.Buffer:
			shard := w.ShardMgr.Get(ackNack.Id.ShardId())
			err := w.MsgRepository.Ack(shard, ackNack.Id, ackNack.Ack)
			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func (w *AckNackWorker) Stop() error {
	errCh := make(chan error)
	w.shutdown <- errCh

	return <-errCh
}
