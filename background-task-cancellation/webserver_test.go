package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// Returns an available TCP port.
//
// Note: there still is a chance the returned port gets allocated
// before the caller binds the port
func getAvailablePort(t *testing.T) int {
	l, err := net.ListenTCP("tcp", nil)
	if err != nil {
		t.Fatalf("could not allocate port: %v", err)
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}

func testHttpRequest(t *testing.T, method string, url string, body io.Reader,
	timeout time.Duration, expectedStatus int) *http.Response {

	reqCtx, reqCancel := context.WithTimeout(context.Background(), time.Second)
	defer reqCancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("error creating http request: %v", err)
	}
	response, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("http request error: %v", err)
	}
	if response.StatusCode != expectedStatus {
		t.Fatalf("wrong status code: expected %d, found %d", http.StatusOK, expectedStatus)
	}

	return response
}

func TestWebServerWithContextCancellation(t *testing.T) {

	port := getAvailablePort(t)
	op := &webServerOp{
		Server: &http.Server{
			Addr: fmt.Sprintf(":%d", port),
		},
	}
	http.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprintf(w, "Healthy")
	})

	ctx, stopServer := context.WithCancel(context.Background())

	completedCh := make(chan struct{})
	go runOpWithContext(ctx, op, completedCh)

	url := fmt.Sprintf("http://localhost:%d/health", port)
	testHttpRequest(t, http.MethodGet, url, nil, time.Second, http.StatusOK)

	stopServer()
	select {
	case <-completedCh:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not shutdown. Operation timed out")
	}
}
