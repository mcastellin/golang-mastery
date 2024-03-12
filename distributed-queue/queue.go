package main

import (
	"fmt"
	"time"

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

type EnqueueResponse struct {
	MsgId domain.UUID
	Err   error
}

type EnqueueRequest struct {
	Msg    domain.Message
	RespCh chan<- EnqueueResponse
}

type EnqueueWorker struct {
	Shard  *domain.ShardMeta
	Buffer chan EnqueueRequest

	shutdown chan chan error
}

func (w *EnqueueWorker) Run() {
	cleanup := func() {
		close(w.shutdown)
	}
	defer cleanup()

	w.shutdown = make(chan chan error)
	for {
		select {
		case respCh := <-w.shutdown:
			respCh <- nil
			return

		case enqReq := <-w.Buffer:
			msg := enqReq.Msg
			var reply EnqueueResponse
			if err := msg.Create(w.Shard); err != nil {
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
	Shard          *domain.ShardMeta
	PrefetchBuffer *prefetch.PriorityBuffer

	shutdown      chan chan error
	topicBackoffs map[string]*wait.BackoffStrategy
}

func (w *DequeueWorker) Run() {
	cleanup := func() {
		close(w.shutdown)
	}
	defer cleanup()

	w.shutdown = make(chan chan error)
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
			msgs, err := domain.SearchMessages(w.Shard, false,
				exclusions, prefetch.MaxPrefetchItemCount, domain.WithLimit(dequeueBatchSize))
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
			tx, err := domain.UpdatePrefetchedBatch(w.Shard, ids, true)
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
	ShardMgr *domain.ShardManager
	Buffer   chan AckNackRequest

	shutdown chan chan error
}

func (w *AckNackWorker) Run() {
	cleanup := func() {
		close(w.shutdown)
	}
	defer cleanup()

	w.shutdown = make(chan chan error)
	for {
		select {
		case respCh := <-w.shutdown:
			respCh <- nil
			return

		case ackNack := <-w.Buffer:
			shard := w.ShardMgr.Get(ackNack.Id.ShardId())
			msg := &domain.Message{Id: ackNack.Id}
			err := msg.Ack(shard, ackNack.Ack)
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
