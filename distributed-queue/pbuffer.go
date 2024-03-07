package main

import (
	"fmt"
	"time"
)

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
	readyCh  chan Message
}

func (pb *PriorityBuffer) Serve() chan DequeueRequest {
	pb.readyCh = make(chan Message, 300)
	pb.apiReqCh = make(chan DequeueRequest, 300)
	go pb.loop()
	return pb.apiReqCh
}

func (pb *PriorityBuffer) loop() {
	for {
		select {

		// Handle dequeue requests from the API by returning items ready for delivery in the prefetch buffer
		case apiReq := <-pb.apiReqCh:
			fmt.Println(apiReq)
			apiReq.replyCh <- DequeueResponse{Messages: []Message{{Payload: []byte("asdlfkasdf")}}}

		// Handle insertion of new messages into the buffer received from the dequeue workers
		case msg := <-pb.readyCh:
			fmt.Println(msg)
		}
	}
}

func (pb *PriorityBuffer) Dequeue(req *DequeueRequest) chan DequeueResponse {
	respCh := make(chan DequeueResponse)
	req.replyCh = respCh

	pb.apiReqCh <- *req

	return respCh
}
