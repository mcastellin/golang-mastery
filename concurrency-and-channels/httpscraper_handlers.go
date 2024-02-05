// Fan-in pattern to pair with HTTP scraper
// This implementation covers the collection of metrics read by the scraper
// into a common data structure. Of course, because we are scraping pages
// concurrently, we need to manage resource contention by either locking the
// resource or by dropping the stats into a collection channel

package main

import (
	"fmt"
	"io"
	"net/http"
	"sync"
)

// Collection of scraped statistics from pages
type ScrapeStats map[string]int

// Handles the scraped response by collecting metrics (for practice's
// sake it's just the lenght of the response body) into a shared data structure.
//
// To synchronize access to the shared `stats` structure we use a Mutex lock.
type ScrapeStatsHandler struct {
	stats ScrapeStats
	mu    sync.RWMutex
}

// Handles the scraped page response and collects metrics
func (sc *ScrapeStatsHandler) Handle(req *http.Request, res *http.Response, err error) {
	if err != nil {
		fmt.Printf("error processing scrape request: %v\n", err)
	}

	b, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Printf("error reading resposne body for %s: %v\n", res.Request.URL, err)
	}
	url := req.URL.String()

	sc.putStat(url, len(b))
}

// Synchronized access to the shared `stats` data structure
func (sc *ScrapeStatsHandler) putStat(key string, value int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.stats == nil {
		sc.stats = ScrapeStats{}
	}
	sc.stats[key] = value
}

// Returns the collected statistics, acquiring a "read-lock" before returning the data
// to make sure it's still consistent even if the method is called during response processing.
func (sc *ScrapeStatsHandler) Stats() ScrapeStats {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.stats
}
