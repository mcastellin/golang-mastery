package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/db"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/domain"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/prefetch"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/queue"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/wait"
	"go.uber.org/zap"
)

type namespaceGetterCreator interface {
	Save(*db.ShardMeta, *domain.Namespace) error
	FindByStringId(*db.ShardMeta, string) (*domain.Namespace, error)
	FindAll(*db.ShardMeta, ...db.OptsFn) ([]domain.Namespace, error)
}

type NamespaceService struct {
	Logger       *zap.Logger
	MainShard    *db.ShardMeta
	NsRepository namespaceGetterCreator
}

type CreateNsRequest struct {
	Name string `json:"name"`
}

func (s *NamespaceService) HandleCreateNamespace(c *ApiCtx) {
	var req CreateNsRequest
	if err := json.NewDecoder(c.Request.Body).Decode(&req); err != nil {
		c.JsonResponse(http.StatusInternalServerError, H{"status": err.Error()})
		return
	}

	item := domain.Namespace{Name: req.Name}
	err := s.NsRepository.Save(s.MainShard, &item)
	if err != nil {
		c.JsonResponse(http.StatusInternalServerError, H{"status": err.Error()})
		return
	}

	err = c.JsonResponse(http.StatusOK, H{
		"id":   item.Id.String(),
		"name": item.Name,
	})
	if err != nil {
		c.JsonResponse(http.StatusInternalServerError, H{"status": err.Error()})
		return
	}
}

func (s *NamespaceService) HandleGetNamespaces(c *ApiCtx) {
	results, err := s.NsRepository.FindAll(s.MainShard, db.WithLimit(100))
	if err != nil {
		c.JsonResponse(http.StatusInternalServerError, H{"status": err.Error()})
		return
	}

	var namespaces []H
	for _, r := range results {
		namespaces = append(namespaces, H{"namespace": r.Id.String(), "name": r.Name})
	}
	c.JsonResponse(http.StatusOK, H{"namespaces": namespaces})
}

type MessagesService struct {
	Logger        *zap.Logger
	MainShard     *db.ShardMeta
	NsRepository  *db.NamespaceRepository
	EnqueueBuffer chan queue.EnqueueRequest
	DequeueBuffer *prefetch.PriorityBuffer
	AckNackRouter *queue.AckNackRouter
}

type EnqueueRequest struct {
	Namespace           string        `json:"namespace"`
	Topic               string        `json:"topic"`
	Priority            uint32        `json:"priority"`
	Payload             string        `json:"payload"`
	Metadata            string        `json:"metadata"`
	DeliverAfterSeconds time.Duration `json:"deliverAfterSeconds"`
	TTLSeconds          time.Duration `json:"ttlSeconds"`
}

func (s *MessagesService) HandleEnqueue(c *ApiCtx) {
	var req EnqueueRequest
	if err := json.NewDecoder(c.Request.Body).Decode(&req); err != nil {
		c.JsonResponse(http.StatusInternalServerError, H{"status": err.Error()})
		return
	}

	ns, err := s.NsRepository.CachedFindByStringId(s.MainShard, req.Namespace)
	if errors.Is(err, sql.ErrNoRows) {
		c.JsonResponse(http.StatusNotFound, H{"error": "invalid namespace"})
		return
	} else if err != nil {
		c.JsonResponse(http.StatusInternalServerError, H{"error": err.Error()})
		return
	}

	msg := domain.Message{
		Namespace:    ns,
		Topic:        req.Topic,
		Priority:     req.Priority,
		Payload:      []byte(req.Payload),
		Metadata:     []byte(req.Metadata),
		DeliverAfter: req.DeliverAfterSeconds * time.Second,
		TTL:          req.TTLSeconds * time.Second,
	}

	respCh := make(chan queue.EnqueueResponse)
	s.EnqueueBuffer <- queue.EnqueueRequest{
		Msg:    msg,
		RespCh: respCh,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		c.JsonResponse(http.StatusNotFound, H{"status": "operation timed out"})
		return

	case resp := <-respCh:
		if resp.Err != nil {
			c.JsonResponse(http.StatusInternalServerError, H{"status": err.Error()})
			return
		}
		c.JsonResponse(http.StatusCreated, H{
			"status": "created",
			"msgId":  resp.MsgId.String(),
		})
	}
}

type DequeueRequest struct {
	Namespace      string `json:"namespace"`
	Topic          string `json:"topic"`
	Limit          int    `json:"limit"`
	TimeoutSeconds int    `json:"timeoutSeconds"`
}

func (s *MessagesService) HandleDequeue(c *ApiCtx) {
	var dequeueReq DequeueRequest
	if err := json.NewDecoder(c.Request.Body).Decode(&dequeueReq); err != nil {
		c.JsonResponse(http.StatusInternalServerError, H{"error": err.Error()})
		return
	}

	r := &prefetch.GetItemsRequest{
		Namespace: dequeueReq.Namespace,
		Topic:     dequeueReq.Topic,
		Limit:     dequeueReq.Limit,
		// TODO check for max allowed timeout
		Timeout: time.Second * time.Duration(dequeueReq.TimeoutSeconds),
	}
	if r.Timeout == 0 {
		r.Timeout = 30 * time.Second
	}

	backoff := wait.NewBackoff(time.Millisecond, 2, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), r.Timeout)
	defer cancel()

	for {
		select {
		case <-backoff.After():
			resp := <-s.DequeueBuffer.GetItems(r)
			if len(resp.Messages) == 0 {
				backoff.Backoff()
				continue
			}

			msgs := []H{}
			for _, m := range resp.Messages {
				msgs = append(msgs, H{
					"id":        m.Id.String(),
					"topic":     m.Topic,
					"namespace": "todo",
					"priority":  m.Priority,
					"payload":   string(m.Payload),
					"metadata":  string(m.Metadata),
				})
			}
			c.JsonResponse(http.StatusOK, H{"messages": msgs})
			return

		case <-ctx.Done():
			c.JsonResponse(http.StatusNotFound, H{"messages": []string{}})
			return
		}
	}
}

type AckNackRequest struct {
	Id  string `json:"id"`
	Ack bool   `json:"ack"`
}

func (s *MessagesService) HandleAckNack(c *ApiCtx) {
	var acks []AckNackRequest
	if err := json.NewDecoder(c.Request.Body).Decode(&acks); err != nil {
		c.JsonResponse(http.StatusInternalServerError, H{"error": err.Error()})
		return
	}

	for _, ack := range acks {
		uid, err := domain.ParseUUID(ack.Id)
		if err != nil {
			s.Logger.Error("error parsing UUID", zap.Error(err))
			continue
		}
		req := queue.AckNackRequest{Id: *uid, Ack: ack.Ack}
		if err := s.AckNackRouter.Route(uid, req); err != nil {
			c.JsonResponse(http.StatusInternalServerError, H{"error": err.Error()})
			return
		}
	}
}
