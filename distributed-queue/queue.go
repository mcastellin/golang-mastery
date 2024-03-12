package main

import (
	"fmt"
)

const (
	dequeueBatchSize = 100
)

type EnqueueResponse struct {
	MsgId UUID
	Err   error
}

type EnqueueRequest struct {
	Msg    Message
	RespCh chan<- EnqueueResponse
}

type EnqueueWorker struct {
	Shard  *ShardMeta
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
	Shard          *ShardMeta
	PrefetchBuffer *PriorityBuffer

	shutdown      chan chan error
	topicBackoffs map[string]*backoffStrategy
}

func (w *DequeueWorker) Run() {
	cleanup := func() {
		close(w.shutdown)
	}
	defer cleanup()

	w.shutdown = make(chan chan error)
	w.topicBackoffs = map[string]*backoffStrategy{}

	retrieveBackoff := newBackoff()
	for {
		select {
		case respCh := <-w.shutdown:
			respCh <- nil
			return
		case <-retrieveBackoff.after():
			//TODO implement backoff strategy for topics.
			// This could be achieved by storing which topics we're asked to backoff and exclude
			// those topics from the search for a certain amount of time
			// Also, to reduce the pressure on the database we could throttle requests and
			// implement some internal backoff strategy if requests came back empty
			// Implementing this worker comes with its own sets of challenges if we want to avoid
			// overwhelming the database
			exclusions := excludedTopics(w.topicBackoffs)
			msgs, err := SearchMessages(w.Shard,
				false, exclusions, 20, WithLimit(dequeueBatchSize))
			if err != nil {
				fmt.Println(err)
				continue
			}

			if len(msgs) == 0 {
				retrieveBackoff.backoff()
				continue
			}
			retrieveBackoff.reset()

			replyCh := make(chan []PrefetchStatusCode)
			envelope := IngestEnvelope{Batch: msgs, RespCh: replyCh}
			w.PrefetchBuffer.C() <- envelope

			reply := <-replyCh
			close(replyCh)

			ids := make([]UUID, 0)
			for i, r := range reply {
				switch r {
				case PrefetchStatusOk:
					ids = append(ids, msgs[i].Id)

				case PrefetchStatusBackoff:
					b := w.topicBackoffs[msgs[i].Topic]
					if b == nil {
						b = newBackoff()
						w.topicBackoffs[msgs[i].Topic] = b
					}
					b.backoff()
				}
			}
			tx, err := UpdatePrefetchedBatch(w.Shard, ids, true)
			if err != nil {
				fmt.Println(err)
				continue
			}
			tx.Commit()
		}
	}
}

func excludedTopics(backoffs map[string]*backoffStrategy) []string {
	excludes := []string{}
	for t, b := range backoffs {
		if !b.active() {
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
