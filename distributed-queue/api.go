package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type H map[string]any

func jsonResponse(w http.ResponseWriter, v H) error {
	w.Header().Add("Content-Type", "application/json")
	err := json.NewEncoder(w).Encode(v)
	return err
}

type APIService struct {
	Handler *Handler
}

func (s *APIService) Serve(ctx context.Context) error {

	mux := http.NewServeMux()
	mux.HandleFunc("/queue", s.Handler.getQueuesHandler)
	mux.HandleFunc("/queue/create", s.Handler.createQueueHandler)
	mux.HandleFunc("/enqueue", s.Handler.enqueueHandler)
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
	ShardMgr      *ShardManager
	EnqueueBuffer chan EnqueueRequest
}

func (hh *Handler) getQueuesHandler(w http.ResponseWriter, r *http.Request) {

	results, err := SearchQueues(hh.ShardMgr)
	if err != nil {
		w.Write([]byte("error"))
		return
	}

	var queues []H
	for _, r := range results {
		queues = append(queues, H{"queueId": r.ID, "name": r.Name})
	}
	jsonResponse(w, H{"queues": queues})
}

type CreateQueueRequest struct {
	Name string `json:"name"`
}

func (hh *Handler) createQueueHandler(w http.ResponseWriter, r *http.Request) {
	var req CreateQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Write([]byte("error"))
		return
	}

	item := Queue{ID: uuid.New(), Name: req.Name}
	err := item.Create(hh.ShardMgr)
	if err != nil {
		w.Write([]byte("error"))
		return
	}

	err = jsonResponse(w, H{
		"queueId": item.ID,
		"name":    item.Name,
	})
	if err != nil {
		w.Write([]byte("error"))
		return
	}
}

type EnqueueReq struct {
	QueueID      uuid.UUID     `json:"queueId"`
	Topic        string        `json:"topic"`
	Priority     uint32        `json:"priority"`
	Payload      string        `json:"payload"`
	Metadata     string        `json:"metadata"`
	DeliverAfter time.Duration `json:"deliverAfter"` // in nanoseconds
	TTL          time.Duration `json:"ttl"`          // in nanoseconds
}

func (hh *Handler) enqueueHandler(w http.ResponseWriter, r *http.Request) {
	var req EnqueueReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Write([]byte("error"))
		return
	}

	q := &Queue{ID: req.QueueID}
	err := q.Get(hh.ShardMgr)
	if errors.Is(err, sql.ErrNoRows) {
		jsonResponse(w, H{"error": "invalid queue"})
		return
	}

	msg := Message{
		Queue:        q,
		Topic:        req.Topic,
		Priority:     req.Priority,
		Payload:      []byte(req.Payload),
		Metadata:     []byte(req.Metadata),
		DeliverAfter: req.DeliverAfter,
		TTL:          req.TTL,
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
			w.Write([]byte("error"))
			return
		}
		jsonResponse(w, H{
			"status": "created",
			"msgId":  resp.MsgId,
		})
	}

}
