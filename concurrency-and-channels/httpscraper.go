package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type requestDoer interface {
	Do(*http.Request) (*http.Response, error)
}

func defaultRequestDoer() requestDoer {
	return &http.Client{}
}

func httpWorker(wg *sync.WaitGroup, reqDoer requestDoer, handler scrapeResponseHandler,
	reqCh <-chan http.Request, sigExit <-chan struct{}, postFn func()) {

	defer wg.Done()

	for {
		select {
		case <-sigExit:
			return
		case req, ok := <-reqCh:
			if !ok {
				return // channel closed
			}
			resp, err := reqDoer.Do(&req)
			handler(resp, err)
			postFn()
		}
	}
}

// Dummy scrape response handler to use if none is provider to the scraper
func defaultScrapeResponseHandler(res *http.Response, err error) {
	if err != nil {
		fmt.Printf("an error occurred while scraping url %s: %v\n", res.Request.URL, err)
	}

	b, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Printf("unparseable body: %v\n", err)
	}
	fmt.Println("received", res.StatusCode, string(b))
}

type httpClientProviderFn func() requestDoer
type scrapeResponseHandler func(*http.Response, error)

// The HTTPScraper is capable of making HTTP requests in parallel using goroutines
// and then call custom handler logic defined by the ResponseHandler function.
//
// Note: HTTPScraper runs requests in goroutines so any handler function used should
// be design not to introduce any race condition
type HTTPScraper struct {
	Workers              int
	Buffer               int
	PageLoadTimeout      time.Duration
	HttpClientProviderFn httpClientProviderFn
	ResponseHandler      scrapeResponseHandler

	scrapedPages int64
	reqCh        chan http.Request
	sigExit      chan struct{}
	closeOnce    sync.Once
	exitOnce     sync.Once
	wg           *sync.WaitGroup
}

// Starts scraper's workers.
//
// This implementation supports context cancellation with graceful termination.
// If context is cancelled, the scraper's input channel is closed and the process will
// exit after in-flight requests are processed. To wait for graceful termination, make
// sure the scraper.Done() method is called.
func (sc *HTTPScraper) Start(ctx context.Context) {

	if sc.Workers <= 0 {
		sc.Workers = 1
	}

	if sc.HttpClientProviderFn == nil {
		sc.HttpClientProviderFn = defaultRequestDoer
	}

	if sc.ResponseHandler == nil {
		sc.ResponseHandler = defaultScrapeResponseHandler
	}

	incrementerFn := func() {
		atomic.AddInt64(&sc.scrapedPages, 1)
	}

	bufSize := sc.Buffer
	if bufSize <= 0 {
		bufSize = sc.Workers * 2
	}
	sc.reqCh = make(chan http.Request, bufSize)
	sc.sigExit = make(chan struct{})

	sc.wg = &sync.WaitGroup{}
	for i := 0; i < sc.Workers; i++ {
		sc.wg.Add(1)
		go httpWorker(sc.wg, sc.HttpClientProviderFn(), sc.ResponseHandler,
			sc.reqCh, sc.sigExit, incrementerFn)
	}

	var exitHandler = func() {
		select {
		case <-ctx.Done():
			sc.exitOnce.Do(func() { close(sc.sigExit) })
		}
	}
	go exitHandler()
}

// Add a new page scraping request into the queue
func (sc *HTTPScraper) Scrape(req http.Request) error {
	select {
	case <-sc.sigExit:
		return fmt.Errorf("scraper close or not started yet.")
	default:
		sc.reqCh <- req
	}
	return nil
}

// Closes the scraper input and blocks until all requests are completed.
//
// After Done() is called, the scraper will be unable to receive further requests.
// Attempting to do so will result in a panic.
func (sc *HTTPScraper) Done(ctx context.Context) {
	sc.closeOnce.Do(func() { close(sc.reqCh) })

	done := make(chan struct{})
	go func() {
		sc.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// graceful termination
	case <-ctx.Done():
		// context cancelled or timed-out
	}
	sc.exitOnce.Do(func() { close(sc.sigExit) })
}

// Returns the total number of pages scraped including failed requests
func (sc *HTTPScraper) ScrapedPages() int64 {
	return atomic.LoadInt64(&sc.scrapedPages)
}
