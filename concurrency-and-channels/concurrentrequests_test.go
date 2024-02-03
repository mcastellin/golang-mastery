package main

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type mockHTTPClient struct {
	Latency time.Duration
}

func (c *mockHTTPClient) Do(*http.Request) (*http.Response, error) {
	time.Sleep(c.Latency)

	return &http.Response{
		StatusCode: http.StatusOK,
		Header: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: io.NopCloser(strings.NewReader("Yay!")),
	}, nil
}

func TestConcurrent(t *testing.T) {

	index := [][]string{
		{"GET", "http://example.com/products/1"},
		{"GET", "http://example.com/products/2"},
		{"GET", "http://example.com/products/3"},
		{"GET", "http://example.com/products/4"},
		{"GET", "http://example.com/products/5"},
		{"GET", "http://acme.com/catalogue/produts/1/details"},
		{"GET", "http://acme.com/catalogue/produts/11/details"},
		{"GET", "http://acme.com/catalogue/produts/1111/details"},
	}

	scraper := &JsonScraper{
		Workers:         10,
		PageLoadTimeout: 30,
		HttpClientProviderFn: func() requestDoer {
			return &mockHTTPClient{Latency: 500 * time.Millisecond}
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	scraper.Start(ctx)

	for _, data := range index {
		req, err := http.NewRequest(data[0], data[1], nil)
		if err != nil {
			t.Fatalf("error creating request: %v", err)
		}
		scraper.Scrape(*req)
	}

	scraper.Done(context.TODO())
	cancel()

	if len(index) != int(scraper.NumScrapedPages()) {
		t.Fatalf("not enough requests processed: expected %d, found %d", len(index), int(scraper.NumScrapedPages()))
	}

}

func TestGracefulTermination(t *testing.T) {

	index := [][]string{
		{"GET", "http://example.com/products/1"},
		{"GET", "http://example.com/products/2"},
		{"GET", "http://example.com/products/3"},
		{"GET", "http://example.com/products/4"},
		{"GET", "http://example.com/products/5"},
		{"GET", "http://acme.com/catalogue/produts/1/details"},
		{"GET", "http://acme.com/catalogue/produts/11/details"},
		{"GET", "http://acme.com/catalogue/produts/1111/details"},
	}

	scraper := &JsonScraper{
		Workers:         1, // no parallel processing
		PageLoadTimeout: 30,
		HttpClientProviderFn: func() requestDoer {
			return &mockHTTPClient{Latency: 500 * time.Millisecond}
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	scraper.Start(ctx)

	for _, data := range index {
		req, err := http.NewRequest(data[0], data[1], nil)
		if err != nil {
			t.Fatalf("error creating request: %v", err)
		}
		if err := scraper.Scrape(*req); err != nil {
			// could be failing because channel is closed
		}
	}

	scraper.Done(context.TODO())

	if len(index) == int(scraper.NumScrapedPages()) {
		t.Fatal("request processing should have terminated early")
	}

}
