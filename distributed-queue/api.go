package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
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
	DequeueBuffer *PriorityBuffer
}

func (hh *Handler) getNamespacesHandler(w http.ResponseWriter, r *http.Request) {

	results, err := SearchNamespaces(hh.MainShard)
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

	item := Namespace{Name: req.Name}
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

	nsuid, err := ParseUUID(req.Namespace)
	if err != nil {
		jsonResponse(w, http.StatusNotFound, H{"error": "invalid namespace id"})
		return
	}
	ns := &Namespace{Id: *nsuid}
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

	msg := Message{
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

func (hh *Handler) dequeueHandler(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case http.MethodGet:
		// TODO

		req := &DequeueRequest{Topic: "test"}
		resp := <-hh.DequeueBuffer.Dequeue(req)
		msgs := []H{}
		for _, m := range resp.Messages {
			msgs = append(msgs, H{
				"id":        m.Id.String(),
				"topic":     m.Topic,
				"namespace": "todo",
				"priority":  m.Priority,
				"payload":   string(m.Payload),
				"Metadata":  string(m.Metadata),
			})
		}
		jsonResponse(w, http.StatusOK, H{"messages": msgs})
	case http.MethodPost:
	// TODO
	default:
		jsonResponse(w, http.StatusBadRequest, H{
			"status": "bad request",
		})
	}
}
