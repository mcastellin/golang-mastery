package queue

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/db"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/domain"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/prefetch"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/wait"
	"go.uber.org/zap"
)

const (
	dequeueBatchSize             = 100
	backoffInitialDuration       = 10 * time.Millisecond
	backoffMaxDuration           = 5 * time.Second
	topicBackoffMaxDuration      = 5 * time.Second
	backoffFactor                = 2
	defaultChanSize              = 300
	responseCommunicationTimeout = 100 * time.Millisecond
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

// NewEnqueueWorker creates a new EnqueueWorker
func NewEnqueueWorker(shard *db.ShardMeta, buf chan EnqueueRequest, logger *zap.Logger) *EnqueueWorker {
	ibuf := buf
	if ibuf == nil {
		ibuf = make(chan EnqueueRequest, defaultChanSize)
	}
	return &EnqueueWorker{
		logger: logger,
		shard:  shard,
		repo:   &db.MessageRepository{},
		buffer: ibuf,
	}
}

// EnqueueWorker implements the worker interface to ingest enqueue requests received
// from producers (clients) and store them into the managed database shard.
//
// I used workers instead of having http handlers directly creating new message records
// to control the amount of concurrent clients that communicate with the database simultaneously.
// API clients can drop the EnqueueRequest into a single buffer and workers will handle the
// record creation asynchronously, one at a time.
// A response is then sent to the caller using the RespCh included in the request.
type EnqueueWorker struct {
	logger *zap.Logger
	shard  *db.ShardMeta
	repo   messageSaver

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
				if enqReq.RespCh == nil {
					// response channel not set. Discarding request
					continue
				}

				reply := w.enqueueMessage(&enqReq.Msg)

				timer := time.NewTimer(responseCommunicationTimeout)
				select {
				case enqReq.RespCh <- reply:
					timer.Stop()
				case <-timer.C:
					// client probably died and didn't pick up the response. Proceeding.
					continue
				}
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

// NewDequeueWorker creates a new DequeueWorker
func NewDequeueWorker(shard *db.ShardMeta, buf *prefetch.PriorityBuffer, logger *zap.Logger) *DequeueWorker {
	return &DequeueWorker{
		logger:      logger,
		shard:       shard,
		repo:        &db.MessageRepository{},
		prefetchBuf: buf,
	}
}

// DequeueWorker implements the worker interface to continuously dequeue messages from the
// database that are ready for delivery.
// On each round, the dequeue worker will fetch messages that met delivery conditions from
// the database. The amount of messages retrieve is bound both globally (overall amount of
// rows fetched) and by topic (at most the N items for every topic sorted by priority).
// The worker will then send messages as part of an ingest request to the PrefetchBuffer.
//
// If the prefetch buffer is full, it can send a "backoff" response to ask workers to slow
// down message retrieval from the database for specific topics.
type DequeueWorker struct {
	logger *zap.Logger
	shard  *db.ShardMeta
	repo   messageSearcherUpdater

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
					w.logger.Error("error fetching messages from database", zap.Error(err))
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
				b = wait.NewBackoff(backoffInitialDuration, backoffFactor, topicBackoffMaxDuration)
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

// NewAckNackWorker creates a new AckNackWorker
func NewAckNackWorker(shard *db.ShardMeta, buf chan AckNackRequest, logger *zap.Logger) *AckNackWorker {
	ibuf := buf
	if buf == nil {
		ibuf = make(chan AckNackRequest, defaultChanSize)
	}
	return &AckNackWorker{
		logger: logger,
		shard:  shard,
		repo:   &db.MessageRepository{},
		buffer: ibuf,
	}
}

// AckNackWorker implements the worker interface to process ACK and NACK requests to messages from API clients.
// Every message consumer has to acknowledge the outcome of a message processing with either an ACK or NACK.
// Because of the sheer amount of ack/nack messages received by the distributed queue, we cannot have http
// handlers updating records in database shards. This operation is handled asynchronously by the worker.
type AckNackWorker struct {
	logger *zap.Logger
	shard  *db.ShardMeta
	repo   messageAckNacker

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
				if err := w.repo.AckNack(w.shard, ackNack.Id, ackNack.Ack); err != nil {
					w.logger.Error("error ack/nack message",
						zap.String("id", ackNack.Id.String()),
						zap.Bool("ack", ackNack.Ack),
						zap.Error(err))
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

// AckNackRouter is responsible for routing an ack/nack request to the correct AckNackWorker
// that can update correct database shard where the message is stored.
// To handle the routing smoothly, we use UUID keys for messages that include shard identification
// so we can route request with a simple map lookup.
type AckNackRouter struct {
	routes map[uint32]chan<- AckNackRequest
}

// RegisterWorker registers a new worker into the router.
func (r *AckNackRouter) RegisterWorker(shardId uint32, w *AckNackWorker) {
	if r.routes == nil {
		r.routes = map[uint32]chan<- AckNackRequest{}
	}
	r.routes[shardId] = w.buffer
}

// Route an incoming ack/nack request to the correct worker buffer for processing.
func (r *AckNackRouter) Route(uid *domain.UUID, req AckNackRequest) error {
	wChan, ok := r.routes[uid.ShardId()]
	if !ok {
		return fmt.Errorf("could not route for uid %s", uid.String())
	}
	wChan <- req
	return nil
}
