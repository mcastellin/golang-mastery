package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestCollectScrapedStats(t *testing.T) {
	index := getUrls(0)

	h := &ScrapeStatsHandler{}

	scraper := &HTTPScraper{
		Workers:         10,
		Buffer:          len(index),
		PageLoadTimeout: 30,
		HttpClientProviderFn: func() requestDoer {
			return &mockHTTPClient{Latency: 500 * time.Millisecond}
		},
		ResponseHandler: h.Handle,
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

	result := h.Stats()

	if len(index) != int(scraper.ScrapedPages()) {
		t.Fatalf("not enough requests processed: expected %d, found %d", len(index), int(scraper.ScrapedPages()))
	}

	expectedResponseLen := 31
	if result[index[0][1]] != expectedResponseLen {
		t.Fatalf("wrong stat collected: expected %d, found %d", expectedResponseLen, result[index[0][1]])
	}
}

func TestHttpScraper(t *testing.T) {
	index := getUrls(0)

	scraper := &HTTPScraper{
		Workers:         10,
		Buffer:          len(index),
		PageLoadTimeout: 30,
		HttpClientProviderFn: func() requestDoer {
			return &mockHTTPClient{Latency: 500 * time.Millisecond}
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	scraper.Start(ctx)

	startTime := time.Now()

	for _, data := range index {
		req, err := http.NewRequest(data[0], data[1], nil)
		if err != nil {
			t.Fatalf("error creating request: %v", err)
		}
		scraper.Scrape(*req)
	}

	scraper.Done(context.TODO())
	cancel()

	stopTime := time.Now()
	elapsed := stopTime.Sub(startTime)

	if elapsed > time.Second {
		t.Fatalf("requests were not processed in parallel: took %s", elapsed)
	}

	if len(index) != int(scraper.ScrapedPages()) {
		t.Fatalf("not enough requests processed: expected %d, found %d", len(index), int(scraper.ScrapedPages()))
	}

}

func TestHTTPScraperGracefulTermination(t *testing.T) {

	index := getUrls(0)

	scraper := &HTTPScraper{
		Workers:         1, // no parallel processing
		Buffer:          len(index),
		PageLoadTimeout: 30,
		HttpClientProviderFn: func() requestDoer {
			return &mockHTTPClient{Latency: 500 * time.Millisecond}
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
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

	if len(index) == int(scraper.ScrapedPages()) {
		t.Fatal("request processing should have terminated early")
	}

}

func TestHTTPScraperGracefulShutdownShouldCancelWithTimeout(t *testing.T) {

	index := getUrls(1)

	scraper := &HTTPScraper{
		Workers:         1, // no parallel processing
		Buffer:          len(index),
		PageLoadTimeout: 30,
		HttpClientProviderFn: func() requestDoer {
			return &mockHTTPClient{Latency: 10 * time.Second}
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	scraper.Start(ctx)

	for _, data := range index {
		req, err := http.NewRequest(data[0], data[1], nil)
		if err != nil {
			t.Fatalf("error creating request: %v", err)
		}
		if err := scraper.Scrape(*req); err != nil {
			// could be failing because of cancellation
		}
	}

	doneCtx, doneCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer doneCancel()

	scraper.Done(doneCtx)

	if len(index) == int(scraper.ScrapedPages()) {
		t.Fatal("request processing should have terminated early")
	}

}

type mockHTTPClient struct {
	Latency time.Duration
}

func (c *mockHTTPClient) Do(*http.Request) (*http.Response, error) {
	time.Sleep(c.Latency)

	bodyString := fmt.Sprintf("{\"productId\":\"%s\",\"stock\":%d}", "1234", 99)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header: map[string][]string{
			"Content-Type": {"application/json"},
		},
		Body: io.NopCloser(strings.NewReader(bodyString)),
	}, nil
}

func getUrls(count int) [][]string {
	urls := [][]string{
		{"GET", "http://example.com/products/1"},
		{"GET", "http://example.com/products/2"},
		{"GET", "http://example.com/products/3"},
		{"GET", "http://example.com/products/4"},
		{"GET", "http://example.com/products/5"},
		{"GET", "http://acme.com/catalogue/produts/1/details"},
		{"GET", "http://acme.com/catalogue/produts/11/details"},
		{"GET", "http://acme.com/catalogue/produts/1111/details"},
	}
	ct := count
	if count <= 0 || count > len(urls) {
		ct = len(urls)
	}
	return urls[0:ct]
}
