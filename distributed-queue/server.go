package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

type H map[string]any

type ApiCtx struct {
	Request *http.Request
	Writer  http.ResponseWriter
}

func (c *ApiCtx) JsonResponse(statusCode int, v H) error {
	c.Writer.Header().Add("Content-Type", "application/json")
	c.Writer.WriteHeader(statusCode)
	err := json.NewEncoder(c.Writer).Encode(v)
	return err
}

func NewApiServer(addr string, basePath string) *ApiServer {
	prefixedBase, err := url.JoinPath(basePath, "/")
	if err != nil {
		prefixedBase = basePath
	}
	return &ApiServer{
		addr:     addr,
		basePath: prefixedBase,
		router:   map[string]func(*ApiCtx){},
	}
}

type ApiServer struct {
	addr     string
	basePath string
	mux      *http.ServeMux
	router   map[string]func(*ApiCtx)
}

func (s *ApiServer) HandleFunc(method string, path string, fn func(*ApiCtx)) {
	if s.mux == nil {
		s.mux = http.NewServeMux()
	}
	fullPath, err := url.JoinPath(s.basePath, path)
	if err != nil {
		// TODO log this out
		fullPath = path
	}
	key := routerKey(method, fullPath)
	s.router[key] = fn
}

func (s *ApiServer) Serve(ctx context.Context) error {
	srv := &http.Server{
		Addr:    s.addr,
		Handler: s.mux,
	}
	router := func(w http.ResponseWriter, r *http.Request) {
		c := &ApiCtx{
			Writer:  w,
			Request: r,
		}
		key := routerKey(r.Method, r.URL.Path)
		fn, ok := s.router[key]
		if !ok {
			c.JsonResponse(http.StatusNotFound, H{"status": "page not found"})
			return
		}

		fn(c)
	}
	s.mux.HandleFunc(s.basePath, router)

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

func routerKey(method string, path string) string {
	return fmt.Sprintf("%s:%s", method, path)
}
