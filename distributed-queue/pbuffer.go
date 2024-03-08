package main

import (
	"container/heap"
	"time"
)

const maxPrefetchItemCount int = 20
const defaultDequeueLimit int = 20

type DequeueRequest struct {
	Namespace string
	Topic     string
	Limit     int
	Timeout   time.Duration

	replyCh chan<- DequeueResponse
}

type DequeueResponse struct {
	Messages []Message
}

type PriorityBuffer struct {
	apiReqCh chan DequeueRequest
	ingestCh chan Message

	// buffers contains one key per fetched topic.
	// Every topic stores a pre-fetch heap with messages
	// that are ready for delivery up to maxPrefetchItemCount
	buffers map[string]*msgHeap
}

func (pb *PriorityBuffer) Serve() chan DequeueRequest {
	pb.apiReqCh = make(chan DequeueRequest, 300)
	pb.ingestCh = make(chan Message, 300)
	if pb.buffers == nil {
		pb.buffers = map[string]*msgHeap{}
	}

	go pb.serveLoop()

	return pb.apiReqCh
}

func (pb *PriorityBuffer) serveLoop() {
	for {
		select {
		case msg := <-pb.ingestCh:
			tHeap, ok := pb.buffers[msg.Topic]
			if !ok {
				newHeap := make(msgHeap, 0)
				tHeap = &newHeap
				heap.Init(tHeap)
				pb.buffers[msg.Topic] = tHeap
			}

			if len(*tHeap) < maxPrefetchItemCount {
				heap.Push(tHeap, &msg)
			}

		case apiReq := <-pb.apiReqCh:
			tHeap, ok := pb.buffers[apiReq.Topic]
			if !ok {
				apiReq.replyCh <- DequeueResponse{Messages: []Message{}}
				continue
			}

			limit := apiReq.Limit
			if limit == 0 {
				limit = defaultDequeueLimit
			}
			n := min(len(*tHeap), limit)
			prefetched := make([]Message, n)
			for i := 0; i < n; i++ {
				item := heap.Pop(tHeap).(*Message)
				prefetched[i] = *item
			}
			apiReq.replyCh <- DequeueResponse{Messages: prefetched}
		}
	}
}

func (pb *PriorityBuffer) C() chan Message {
	return pb.ingestCh
}

func (pb *PriorityBuffer) Dequeue(req *DequeueRequest) chan DequeueResponse {
	respCh := make(chan DequeueResponse)
	req.replyCh = respCh

	pb.apiReqCh <- *req

	return respCh
}

type msgHeap []*Message

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
	item := v.(*Message)
	*mh = append(*mh, item)
}

func (mh *msgHeap) Pop() any {
	old := *mh
	n := len(old)
	item := old[n-1]
	*mh = old[:n-1]
	return item
}
