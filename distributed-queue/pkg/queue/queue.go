package queue

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
	defaultChanSize        = 300
)

type messageSaver interface {
	Save(*db.ShardMeta, *domain.Message) error
}
type messageAckNacker interface {
	AckNack(*db.ShardMeta, domain.UUID, bool) error
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

func NewEnqueueWorker(shard *db.ShardMeta, buf chan EnqueueRequest) *EnqueueWorker {
	ibuf := buf
	if ibuf == nil {
		ibuf = make(chan EnqueueRequest, defaultChanSize)
	}
	return &EnqueueWorker{
		shard:  shard,
		repo:   &db.MessageRepository{},
		buffer: ibuf,
	}
}

type EnqueueWorker struct {
	shard *db.ShardMeta
	repo  messageSaver

	buffer chan EnqueueRequest

	shutdown chan chan error
}

func (w *EnqueueWorker) Run() error {
	w.shutdown = make(chan chan error)
	cleanup := func() {
		close(w.shutdown)
	}

	runLoop := func() {
		defer cleanup()
		for {
			select {
			case respCh := <-w.shutdown:
				respCh <- nil
				return

			case enqReq := <-w.buffer:
				reply := w.enqueueMessage(&enqReq.Msg)
				enqReq.RespCh <- reply
			}
		}
	}
	go runLoop()
	return nil
}

func (w *EnqueueWorker) enqueueMessage(msg *domain.Message) EnqueueResponse {
	var reply EnqueueResponse
	if err := w.repo.Save(w.shard, msg); err != nil {
		reply.Err = err
		return reply
	}
	reply.MsgId = msg.Id
	return reply
}

func (w *EnqueueWorker) Stop() error {
	errCh := make(chan error)
	w.shutdown <- errCh

	return <-errCh
}

func NewDequeueWorker(shard *db.ShardMeta, buf *prefetch.PriorityBuffer) *DequeueWorker {
	return &DequeueWorker{
		shard:       shard,
		repo:        &db.MessageRepository{},
		prefetchBuf: buf,
	}
}

type DequeueWorker struct {
	shard *db.ShardMeta
	repo  messageSearcherUpdater

	prefetchBuf *prefetch.PriorityBuffer

	shutdown      chan chan error
	topicBackoffs map[string]*wait.BackoffStrategy
}

func (w *DequeueWorker) Run() error {
	w.shutdown = make(chan chan error)
	cleanup := func() {
		close(w.shutdown)
	}

	runLoop := func() {
		defer cleanup()
		w.topicBackoffs = map[string]*wait.BackoffStrategy{}
		loopBackoff := wait.NewBackoff(backoffInitialDuration, backoffFactor, backoffMaxDuration)
		for {
			select {
			case respCh := <-w.shutdown:
				respCh <- nil
				return
			case <-loopBackoff.After():
				if err := w.dequeueMessages(loopBackoff); err != nil {
					fmt.Printf("%v\n", err)
				}
			}
		}
	}
	go runLoop()
	return nil
}

func (w *DequeueWorker) dequeueMessages(bo *wait.BackoffStrategy) error {
	exclusions := excludedTopics(w.topicBackoffs)
	msgs, err := w.repo.FindMessagesReadyForDelivery(w.shard, false,
		exclusions, prefetch.MaxPrefetchItemCount, db.WithLimit(dequeueBatchSize))
	if err != nil {
		return err
	}

	if len(msgs) == 0 {
		bo.Backoff()
		return nil
	}
	bo.Reset()

	fetchedIds := w.sendToPrefetchBuffer(msgs)

	tx, err := w.repo.UpdatePrefetchedBatch(w.shard, fetchedIds, true)
	if err != nil {
		return err
	}
	tx.Commit()

	return nil
}

func (w *DequeueWorker) sendToPrefetchBuffer(items []domain.Message) []domain.UUID {
	replyCh := make(chan []prefetch.PrefetchResponseStatus)
	defer close(replyCh)

	envelope := prefetch.IngestEnvelope{Batch: items, RespCh: replyCh}
	w.prefetchBuf.C() <- envelope

	reply := <-replyCh

	return w.processPrefetchResponse(items, reply)
}

func (w *DequeueWorker) processPrefetchResponse(items []domain.Message, reply []prefetch.PrefetchResponseStatus) []domain.UUID {
	fetchedIds := make([]domain.UUID, 0)
	for i, r := range reply {
		switch r {
		case prefetch.PrefetchStatusOk:
			fetchedIds = append(fetchedIds, items[i].Id)

		case prefetch.PrefetchStatusBackoff:
			b := w.topicBackoffs[items[i].Topic]
			if b == nil {
				b = wait.NewBackoff(backoffInitialDuration, backoffFactor, backoffMaxDuration)
				w.topicBackoffs[items[i].Topic] = b
			}
			b.Backoff()
		}
	}
	return fetchedIds
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

func NewAckNackWorker(shard *db.ShardMeta, buf chan AckNackRequest) *AckNackWorker {
	ibuf := buf
	if buf == nil {
		ibuf = make(chan AckNackRequest, defaultChanSize)
	}
	return &AckNackWorker{
		shard:  shard,
		repo:   &db.MessageRepository{},
		buffer: ibuf,
	}
}

type AckNackWorker struct {
	shard *db.ShardMeta
	repo  messageAckNacker

	buffer chan AckNackRequest

	shutdown chan chan error
}

func (w *AckNackWorker) Run() error {
	w.shutdown = make(chan chan error)
	cleanup := func() {
		close(w.shutdown)
	}

	runLoop := func() {
		defer cleanup()
		for {
			select {
			case respCh := <-w.shutdown:
				respCh <- nil
				return

			case ackNack := <-w.buffer:
				err := w.repo.AckNack(w.shard, ackNack.Id, ackNack.Ack)
				if err != nil {
					fmt.Println(err)
				}
			}
		}
	}
	go runLoop()
	return nil
}

func (w *AckNackWorker) Stop() error {
	errCh := make(chan error)
	w.shutdown <- errCh

	return <-errCh
}

type AckNackRouter struct {
	routes map[uint32]chan<- AckNackRequest
}

func (r *AckNackRouter) RegisterWorker(shardId uint32, w *AckNackWorker) {
	if r.routes == nil {
		r.routes = map[uint32]chan<- AckNackRequest{}
	}
	r.routes[shardId] = w.buffer
}

func (r *AckNackRouter) Route(uid *domain.UUID, req AckNackRequest) error {
	wChan, ok := r.routes[uid.ShardId()]
	if !ok {
		return fmt.Errorf("could not route for uid %s", uid.String())
	}
	wChan <- req
	return nil
}
