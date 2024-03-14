package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

// H is inspired by the gin.H struct, just a shorthand for a map type
type H map[string]any

// ApiCtx represents the context of an API request
type ApiCtx struct {
	Request *http.Request
	Writer  http.ResponseWriter
}

// JsonResponse is a utility function to write a JSON response with its associated
// status code to the ResponseWriter
func (c *ApiCtx) JsonResponse(statusCode int, v H) error {
	c.Writer.Header().Add("Content-Type", "application/json")
	c.Writer.WriteHeader(statusCode)
	err := json.NewEncoder(c.Writer).Encode(v)
	return err
}

// NewApiServer initializes an ApiServer struct
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

// ApiServer represents the state of the API router
type ApiServer struct {
	addr     string
	basePath string
	mux      *http.ServeMux
	router   map[string]func(*ApiCtx)
}

// HandleFunc adds a new handler to the router to handle requests with
// matching method and URL path.
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

// Serve listens for incoming HTTP requests on the specified bind addr
// and routes them to the appropriate function for handling.
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

// routerKey is an internal function to build the key used by the router to
// match handler functions
func routerKey(method string, path string) string {
	return fmt.Sprintf("%s:%s", method, path)
}
