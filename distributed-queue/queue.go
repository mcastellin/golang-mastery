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

	shutdown chan chan error
}

func (w *DequeueWorker) Run() {
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
		default:
			//TODO implement backoff strategy for topics.
			// This could be achieved by storing which topics we're asked to backoff and exclude
			// those topics from the search for a certain amount of time
			// Also, to reduce the pressure on the database we could throttle requests and
			// implement some internal backoff strategy if requests came back empty
			// Implementing this worker comes with its own sets of challenges if we want to avoid
			// overwhelming the database
			msgs, err := SearchMessages(w.Shard, false, 20, WithLimit(dequeueBatchSize))
			if err != nil {
				fmt.Println(err)
				continue
			}

			if len(msgs) == 0 {
				continue
			}

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
					// TODO implement me
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

func (w *DequeueWorker) Stop() error {
	errCh := make(chan error)
	w.shutdown <- errCh

	return <-errCh
}
