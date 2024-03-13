package prefetch

import (
	"testing"
	"time"

	"github.com/mcastellin/golang-mastery/distributed-queue/pkg/domain"
)

func TestBuffer(t *testing.T) {
	buf := NewPriorityBuffer()
	buf.Run()
	defer buf.Stop()

	testMessages := []domain.Message{
		{Topic: "test", Priority: 10},
		{Topic: "test", Priority: 91928347},
		{Topic: "test", Priority: 700},
		{Topic: "test", Priority: 1},
	}

	notificationCh := make(chan bool)
	respCh := make(chan []PrefetchResponseStatus)
	buf.C() <- IngestEnvelope{Batch: testMessages, RespCh: respCh}
	<-respCh
	close(respCh)

	consumer := func() {
		consumed := 0
		for {
			req := &GetItemsRequest{
				Namespace: "ns",
				Topic:     "test",
			}

			reply := <-buf.GetItems(req)
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
