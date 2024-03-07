package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
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
	MainShard     *ShardMeta
	EnqueueBuffer chan EnqueueRequest
}

func (hh *Handler) getQueuesHandler(w http.ResponseWriter, r *http.Request) {

	results, err := SearchQueues(hh.MainShard)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	var queues []H
	for _, r := range results {
		queues = append(queues, H{"queueId": r.Id.String(), "name": r.Name})
	}
	jsonResponse(w, H{"queues": queues})
}

type CreateQueueRequest struct {
	Name string `json:"name"`
}

func (hh *Handler) createQueueHandler(w http.ResponseWriter, r *http.Request) {
	var req CreateQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	item := Queue{Name: req.Name}
	err := item.Create(hh.MainShard)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	err = jsonResponse(w, H{
		"queueId": item.Id.String(),
		"name":    item.Name,
	})
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}
}

type EnqueueReq struct {
	QueueID      string        `json:"queueId"`
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
		w.Write([]byte(err.Error()))
		return
	}

	quid, err := ParseUUID(req.QueueID)
	if err != nil {
		jsonResponse(w, H{"error": "invalid queue id"})
		return
	}
	q := &Queue{Id: *quid}
	err = q.Get(hh.MainShard)
	if errors.Is(err, sql.ErrNoRows) {
		jsonResponse(w, H{"error": "invalid queue"})
		return
	} else if err != nil {
		jsonResponse(w, H{"error": err.Error()})
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
			w.Write([]byte(resp.Err.Error()))
			return
		}
		jsonResponse(w, H{
			"status": "created",
			"msgId":  resp.MsgId.String(),
		})
	}

}
