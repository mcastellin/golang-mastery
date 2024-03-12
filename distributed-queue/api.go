package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/domain"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/prefetch"
	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/wait"
)

type H map[string]any

func jsonResponse(w http.ResponseWriter, statusCode int, v H) error {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	err := json.NewEncoder(w).Encode(v)
	return err
}

type APIService struct {
	Handler *Handler
}

func (s *APIService) Serve(ctx context.Context) error {

	mux := http.NewServeMux()
	mux.HandleFunc("/ns", s.Handler.getNamespacesHandler)
	mux.HandleFunc("/ns/create", s.Handler.createNsHandler)
	mux.HandleFunc("/enqueue", s.Handler.enqueueHandler)
	mux.HandleFunc("/dequeue", s.Handler.dequeueHandler)
	mux.HandleFunc("/message/ack", s.Handler.ackNackHandler)
	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	go func() {
		select {
		case <-ctx.Done():
			if err := srv.Shutdown(context.TODO()); err != nil {
				panic(err)
			}
		}
	}()
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

type Handler struct {
	ShardMgr      *domain.ShardManager
	MainShard     *domain.ShardMeta
	EnqueueBuffer chan EnqueueRequest
	DequeueBuffer *prefetch.PriorityBuffer
	AckNackBuffer chan<- AckNackRequest
}

func (hh *Handler) getNamespacesHandler(w http.ResponseWriter, r *http.Request) {

	results, err := domain.SearchNamespaces(hh.MainShard)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	var namespaces []H
	for _, r := range results {
		namespaces = append(namespaces, H{"namespace": r.Id.String(), "name": r.Name})
	}
	jsonResponse(w, http.StatusOK, H{"namespaces": namespaces})
}

type CreateNsRequest struct {
	Name string `json:"name"`
}

func (hh *Handler) createNsHandler(w http.ResponseWriter, r *http.Request) {
	var req CreateNsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	item := domain.Namespace{Name: req.Name}
	err := item.Create(hh.MainShard)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	err = jsonResponse(w, http.StatusOK, H{
		"id":   item.Id.String(),
		"name": item.Name,
	})
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
}

type EnqueueReq struct {
	Namespace           string        `json:"namespace"`
	Topic               string        `json:"topic"`
	Priority            uint32        `json:"priority"`
	Payload             string        `json:"payload"`
	Metadata            string        `json:"metadata"`
	DeliverAfterSeconds time.Duration `json:"deliverAfterSeconds"`
	TTLSeconds          time.Duration `json:"ttlSeconds"`
}

func (hh *Handler) enqueueHandler(w http.ResponseWriter, r *http.Request) {
	var req EnqueueReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	nsuid, err := domain.ParseUUID(req.Namespace)
	if err != nil {
		jsonResponse(w, http.StatusNotFound, H{"error": "invalid namespace id"})
		return
	}
	ns := &domain.Namespace{Id: *nsuid}
	// TODO need to replicate this in every shard to maintain performance
	// for now just disabling queue verification
	//
	//err = q.Get(hh.MainShard)
	//if errors.Is(err, sql.ErrNoRows) {
	//jsonResponse(w, H{"error": "invalid queue"})
	//return
	//} else if err != nil {
	//jsonResponse(w, H{"error": err.Error()})
	//return
	//}

	msg := domain.Message{
		Namespace:    ns,
		Topic:        req.Topic,
		Priority:     req.Priority,
		Payload:      []byte(req.Payload),
		Metadata:     []byte(req.Metadata),
		DeliverAfter: req.DeliverAfterSeconds * time.Second,
		TTL:          req.TTLSeconds * time.Second,
	}

	respCh := make(chan EnqueueResponse)
	hh.EnqueueBuffer <- EnqueueRequest{
		Msg:    msg,
		RespCh: respCh,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	select {
	case <-ctx.Done():
		w.Write([]byte("operation timed out"))
		return

	case resp := <-respCh:
		if resp.Err != nil {
			fmt.Println(resp.Err)
			w.Write([]byte(resp.Err.Error()))
			return
		}
		jsonResponse(w, http.StatusCreated, H{
			"status": "created",
			"msgId":  resp.MsgId.String(),
		})
	}

}

type DequeueReq struct {
	Namespace      string `json:"namespace"`
	Topic          string `json:"topic"`
	Limit          int    `json:"limit"`
	TimeoutSeconds int    `json:"timeoutSeconds"`
}

func (hh *Handler) dequeueHandler(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		// TODO
	case http.MethodPost:
		var dequeueReq DequeueReq
		if err := json.NewDecoder(req.Body).Decode(&dequeueReq); err != nil {
			jsonResponse(w, http.StatusInternalServerError, H{"error": err.Error()})
		}

		r := &prefetch.DequeueRequest{
			Namespace: dequeueReq.Namespace,
			Topic:     dequeueReq.Topic,
			Limit:     dequeueReq.Limit,
			// TODO check for max allowed timeout
			Timeout: time.Second * time.Duration(dequeueReq.TimeoutSeconds),
		}
		if r.Timeout == 0 {
			r.Timeout = 30 * time.Second
		}

		backoff := wait.NewBackoff(time.Millisecond, 10, time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), r.Timeout)
		defer cancel()

		for {
			select {
			case <-backoff.After():
				resp := <-hh.DequeueBuffer.Dequeue(r)
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
				jsonResponse(w, http.StatusOK, H{"messages": msgs})
				return

			case <-ctx.Done():
				jsonResponse(w, http.StatusNotFound, H{
					"messages": []string{},
				})
				return
			}
		}
	default:
		jsonResponse(w, http.StatusBadRequest, H{
			"status": "bad request",
		})
	}
}

type AckNackReq struct {
	Id  string `json:"id"`
	Ack bool   `json:"ack"`
}

func (hh *Handler) ackNackHandler(w http.ResponseWriter, req *http.Request) {
	var acks []AckNackReq
	if err := json.NewDecoder(req.Body).Decode(&acks); err != nil {
		fmt.Println(err)
		w.Write([]byte(err.Error()))
		return
	}

	for _, ack := range acks {
		uid, err := domain.ParseUUID(acks[0].Id)
		if err != nil {
			fmt.Println(err)
			continue
		}
		req := AckNackRequest{Id: *uid, Ack: ack.Ack}
		hh.AckNackBuffer <- req
	}
}
