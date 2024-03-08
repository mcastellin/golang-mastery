package main

import "time"

type EnqueueResponse struct {
	MsgId UUID
	Err   error
}

type EnqueueRequest struct {
	Msg    Message
	RespCh chan<- EnqueueResponse
}

type EnqueueWorker struct {
	Shard  *ShardMeta
	Buffer chan EnqueueRequest

	shutdown chan chan error
}

func (w *EnqueueWorker) Run() {
	w.shutdown = make(chan chan error)
	for {
		select {
		case respCh := <-w.shutdown:
			respCh <- nil
			return

		case enqReq := <-w.Buffer:
			msg := enqReq.Msg
			var reply EnqueueResponse
			if err := msg.Create(w.Shard); err != nil {
				reply.Err = err
				enqReq.RespCh <- reply
				continue
			}
			reply.MsgId = msg.Id
			enqReq.RespCh <- reply
		}
	}
}

func (w *EnqueueWorker) Stop() error {
	errCh := make(chan error)
	w.shutdown <- errCh

	return <-errCh
}

type DequeueWorker struct {
	Shard          *ShardMeta
	PrefetchBuffer *PriorityBuffer

	shutdown chan chan error
}

func (w *DequeueWorker) Run() {
	w.shutdown = make(chan chan error)
	for {
		select {
		case respCh := <-w.shutdown:
			respCh <- nil
			return
		default:
			// TODO this is just test code
			time.Sleep(100 * time.Millisecond)
			w.PrefetchBuffer.C() <- Message{
				Id:           NewUUID(w.Shard.Id),
				Topic:        "test",
				Priority:     1,
				Payload:      []byte("asdfhnadvuasdfa"),
				Metadata:     []byte("asdfhnadvuasdfa"),
				DeliverAfter: time.Second,
				TTL:          time.Minute,
			}
		}
	}
}

func (w *DequeueWorker) Stop() error {
	errCh := make(chan error)
	w.shutdown <- errCh

	return <-errCh
}
