package prefetch

import (
	"container/heap"
	"fmt"
	"time"

	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/domain"
)

const (
	// MaxPrefetchItemCount is the maximum number of items the buffer
	// will prefetch for every topic
	MaxPrefetchItemCount = 100

	defaultDequeueLimitPerTopic = 20
	defaultChanSize             = 300

	responseCommunicationTimeout = 100 * time.Millisecond
)

// PrefetchResponseStatus is a status code the prefetch buffer will use
// to respond to prefetch requests from dequeue workers.
// If a worker is fetching items faster than consumers are pulling messages,
// the buffer will fill up to the MaxPrefetchItemCount and start rjecting items
// with a "backoff" status code.
type PrefetchResponseStatus int

// String representation of the PrefetchResponseStatus
func (ps *PrefetchResponseStatus) String() string {
	switch *ps {
	case PrefetchStatusOk:
		return "ok"
	case PrefetchStatusBackoff:
		return "backoff"
	default:
		return "unknown"
	}
}

const (
	PrefetchStatusOk      PrefetchResponseStatus = 0 // item accepted by the buffer
	PrefetchStatusBackoff PrefetchResponseStatus = 1 // buffer full, workers should backoff
)

// GetItemsRequest is a request structure used by API clients to ask for messages that are ready
// for delivery.
// GetitemsRequests are buffered and will be processed by the PriorityBuffer asynchronously. Requests
// must contain an initialized replyCh to receive a response from the buffer.
type GetItemsRequest struct {
	Namespace string
	Topic     string
	Limit     int
	Timeout   time.Duration

	replyCh chan<- GetItemsResponse
}

// GetItemResponse is a response structure to send prefetched messages to clients.
type GetItemsResponse struct {
	Messages []domain.Message
}

// IngestEnvelope is a structure received by the prefetch workers to load pre-fetched messages
// from the database that are ready for delivery into the buffer.
type IngestEnvelope struct {
	Batch  []domain.Message
	RespCh chan<- []PrefetchResponseStatus
}

// NewPriorityBuffer creates a new PriorityBuffer struct.
func NewPriorityBuffer() *PriorityBuffer {
	return &PriorityBuffer{
		apiReqCh: make(chan GetItemsRequest, defaultChanSize),
		ingestCh: make(chan IngestEnvelope, defaultChanSize),
	}
}

// PriorityBuffer implements the worker interface and is used to pre-fetch messages in-memory
// for faster delivery to clients.
// A certain number of items is prefetched for each topic that has messages that are ready to be delivered.
type PriorityBuffer struct {
	apiReqCh chan GetItemsRequest
	ingestCh chan IngestEnvelope

	// buffers contains one key per fetched topic.
	// Every topic stores a pre-fetch heap with messages
	// that are ready for delivery up to MaxPrefetchItemCount
	buffers  map[string]*msgHeap
	shutdown chan chan error
}

// C returns the ingest channel that receives messages from the prefetch workers.
func (pb *PriorityBuffer) C() chan IngestEnvelope {
	if pb.ingestCh == nil {
		panic(fmt.Errorf("chan not initialized"))
	}
	return pb.ingestCh
}

// Run the prefetch worker loop
func (pb *PriorityBuffer) Run() error {
	pb.shutdown = make(chan chan error)

	if pb.buffers == nil {
		pb.buffers = map[string]*msgHeap{}
	}
	go pb.serveLoop()

	return nil
}

// serveLoop is an internal routine that receives items from prefetch workers and
// sends messages to API clients.
// Both functions are implemented in the same loop because they need to access the same
// data structure, hence running them in separate goroutines would require mutex locking
// to avoid data races.
func (pb *PriorityBuffer) serveLoop() {
	cleanup := func() {
		pb.buffers = nil
		close(pb.shutdown)
	}
	defer cleanup()

	for {
		select {
		case respCh := <-pb.shutdown:
			respCh <- nil
			return

		case envelope := <-pb.ingestCh:
			reply := pb.processIngest(&envelope)
			envelope.RespCh <- reply

		case apiReq := <-pb.apiReqCh:
			if apiReq.replyCh == nil {
				// can't send replies to an empty channel. rejecting
				continue
			}

			reply := pb.processGetItems(&apiReq)

			select {
			case apiReq.replyCh <- *reply:
			case <-time.After(responseCommunicationTimeout):
				// Delivery failed. Avoid blocking loop.
				// Once msg lease expires will be fetched again for delivery
				// if supported.
				continue
			}
		}
	}
}

func (pb *PriorityBuffer) processGetItems(req *GetItemsRequest) *GetItemsResponse {
	tHeap, ok := pb.buffers[req.Topic]
	if !ok {
		return &GetItemsResponse{Messages: []domain.Message{}}
	}

	limit := req.Limit
	if limit == 0 {
		limit = defaultDequeueLimitPerTopic
	}
	n := min(len(*tHeap), limit)

	prefetched := make([]domain.Message, n)
	for i := 0; i < n; i++ {
		item := heap.Pop(tHeap).(*domain.Message)
		prefetched[i] = *item
	}
	return &GetItemsResponse{Messages: prefetched}
}

func (pb *PriorityBuffer) processIngest(envelope *IngestEnvelope) []PrefetchResponseStatus {
	reply := make([]PrefetchResponseStatus, len(envelope.Batch))

	for i := 0; i < len(envelope.Batch); i++ {
		msg := envelope.Batch[i]
		tHeap, ok := pb.buffers[msg.Topic]
		if !ok {
			newHeap := make(msgHeap, 0)
			tHeap = &newHeap
			heap.Init(tHeap)
			pb.buffers[msg.Topic] = tHeap
		}

		if len(*tHeap) < MaxPrefetchItemCount {
			heap.Push(tHeap, &msg)
			reply[i] = PrefetchStatusOk
		} else {
			reply[i] = PrefetchStatusBackoff
		}
	}
	return reply
}

// Stop the worker loop
func (pb *PriorityBuffer) Stop() error {
	errCh := make(chan error)
	pb.shutdown <- errCh

	return <-errCh
}

// GetItems places a new GetItemRequest into the worker's buffer and returns
// a chan that the caller can use to receive the response asynchronously
func (pb *PriorityBuffer) GetItems(req *GetItemsRequest) chan GetItemsResponse {
	respCh := make(chan GetItemsResponse)
	req.replyCh = respCh

	pb.apiReqCh <- *req

	return respCh
}

// msgHeap is an implementation of the heap.Interface that allows us to
// store prefetched messages in a priority tree
type msgHeap []*domain.Message

func (mh msgHeap) Len() int {
	return len(mh)
}

func (mh msgHeap) Less(i, j int) bool {
	return mh[i].Priority < mh[j].Priority
}

func (mh msgHeap) Swap(i, j int) {
	mh[i], mh[j] = mh[j], mh[i]
}

func (mh *msgHeap) Push(v any) {
	item := v.(*domain.Message)
	*mh = append(*mh, item)
}

func (mh *msgHeap) Pop() any {
	old := *mh
	n := len(old)
	item := old[n-1]
	*mh = old[:n-1]
	return item
}
