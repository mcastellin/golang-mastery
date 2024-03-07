package main

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

type DequeueWorker struct{}
