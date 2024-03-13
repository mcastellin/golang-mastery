package prefetch

import (
	"container/heap"
	"fmt"
	"time"

	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/domain"
)

const (
	MaxPrefetchItemCount        int = 100
	defaultDequeueLimitPerTopic int = 20
	defaultChanSize             int = 300
)

type PrefetchResponseStatus int

const (
	PrefetchStatusOk      PrefetchResponseStatus = 0
	PrefetchStatusBackoff PrefetchResponseStatus = 1
)

type GetItemsRequest struct {
	Namespace string
	Topic     string
	Limit     int
	Timeout   time.Duration

	replyCh chan<- GetItemsResponse
}

type GetItemsResponse struct {
	Messages []domain.Message
}

type IngestEnvelope struct {
	Batch  []domain.Message
	RespCh chan<- []PrefetchResponseStatus
}

func NewPriorityBuffer() *PriorityBuffer {
	return &PriorityBuffer{
		apiReqCh: make(chan GetItemsRequest, defaultChanSize),
		ingestCh: make(chan IngestEnvelope, defaultChanSize),
	}
}

type PriorityBuffer struct {
	apiReqCh chan GetItemsRequest
	ingestCh chan IngestEnvelope

	// buffers contains one key per fetched topic.
	// Every topic stores a pre-fetch heap with messages
	// that are ready for delivery up to MaxPrefetchItemCount
	buffers  map[string]*msgHeap
	shutdown chan chan error
}

func (pb *PriorityBuffer) C() chan IngestEnvelope {
	if pb.ingestCh == nil {
		panic(fmt.Errorf("chan not initialized"))
	}
	return pb.ingestCh
}

func (pb *PriorityBuffer) Run() error {
	pb.shutdown = make(chan chan error)

	if pb.buffers == nil {
		pb.buffers = map[string]*msgHeap{}
	}
	go pb.serveLoop()

	return nil
}

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
			reply := pb.processGetItems(&apiReq)
			apiReq.replyCh <- *reply
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

func (pb *PriorityBuffer) Stop() error {
	errCh := make(chan error)
	pb.shutdown <- errCh

	return <-errCh
}

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
