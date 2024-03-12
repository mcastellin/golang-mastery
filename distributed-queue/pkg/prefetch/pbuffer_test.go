package prefetch

import (
	"testing"
	"time"

	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/domain"
)

func TestBuffer(t *testing.T) {
	buf := &PriorityBuffer{}
	buf.Serve()

	testMessages := []domain.Message{
		{Topic: "test", Priority: 10},
		{Topic: "test", Priority: 91928347},
		{Topic: "test", Priority: 700},
		{Topic: "test", Priority: 1},
	}

	notificationCh := make(chan bool)
	for _, m := range testMessages {
		buf.C() <- m
	}

	consumer := func() {
		consumed := 0
		for {
			req := &DequeueRequest{
				Namespace: "ns",
				Topic:     "test",
			}

			reply := <-buf.Dequeue(req)
			consumed += len(reply.Messages)

			lastPriority := -1
			for _, m := range reply.Messages {
				if lastPriority < 0 {
					lastPriority = int(m.Priority)
					continue
				}

				if lastPriority > int(m.Priority) {
					notificationCh <- false
					return
				}
			}

			if consumed == len(testMessages) {
				notificationCh <- true
				return
			}
		}
	}

	go consumer()

	select {
	case <-time.After(time.Second):
		// escape test after 1 second
		t.Fatal("test timed out")
	case result := <-notificationCh:
		if !result {
			t.Fatal("priority order was not honoured")
		}
	}
}