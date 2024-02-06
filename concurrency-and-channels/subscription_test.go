package main

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func readEvents(events []Event, sub string) {
	for _, e := range events {
		fmt.Printf("T: %s, S: %s -> %s\n", e.TopicName, sub, e.Content)
	}
}

func consumeSubscription(s Subscription, name string, handler func([]Event, string)) {
	for e := range s.Updates() {
		if handler == nil {
			handler = readEvents
		}
		handler(e, name)
	}
}

// Simple test that all subscribers are receiving updates.
func TestEventsHandling(t *testing.T) {
	topic := NewTopic("security alert")
	defer topic.Close()

	email := topic.Subscribe()
	logging := topic.Subscribe()

	var emailSent int32 = 0
	emailHandler := func(events []Event, sub string) {
		atomic.AddInt32(&emailSent, int32(len(events)))
	}
	go consumeSubscription(email, "email", emailHandler)

	var logLines int32 = 0
	loggingHandler := func(events []Event, sub string) {
		atomic.AddInt32(&logLines, int32(len(events)))
	}
	go consumeSubscription(logging, "logging", loggingHandler)

	<-time.After(100 * time.Millisecond)
	topic.Push("security alert: IAM breach")
	<-time.After(time.Second)

	if emailSent != 1 {
		t.Fatalf("Sould have received 1 email notification: got %d", emailSent)
	}
	if logLines != 1 {
		t.Fatalf("Sould have received 1 logging notification: got %d", logLines)
	}
}

// Test that when a bad subscriber joins a topic, all other subscribers are unaffected
//
// A bad subscriber is a routine that joined the Topic but is not consuming Events quickly
// enough (or at all). In such a case all other subscribers should still be receiving
// updates regularly.
func TestBadSubscriber(t *testing.T) {
	topic := NewTopic("security alert")
	defer topic.Close()

	email := topic.Subscribe()
	topic.Subscribe() // bad subscriber

	var emailSent int32 = 0
	emailHandler := func(events []Event, sub string) {
		atomic.AddInt32(&emailSent, int32(len(events)))
	}
	go consumeSubscription(email, "email", emailHandler)

	<-time.After(100 * time.Millisecond)
	expectedNotifications := 200
	for i := 0; i < expectedNotifications; i++ {
		// generate one notification every 10 milliseconds
		topic.Push("security alert: IAM breach")
		time.Sleep(10 * time.Millisecond)
	}
	<-time.After(time.Second) // allow some time to finish processing events

	if emailSent != int32(expectedNotifications) {
		t.Fatalf("Sould have received %d email notification: got %d", expectedNotifications, emailSent)
	}
}

// A simple test of message throughput with a considerable number of subscribers.
func TestSubscriptionThroughput(t *testing.T) {
	topic := NewTopic("t1")

	genClosing := make(chan chan error)
	defer func() {
		c := make(chan error)
		genClosing <- c
		<-c
	}()
	eventGenerator := func() {
		counter := 0
		for {
			select {
			case <-time.After(100 * time.Millisecond):
				topic.Push(fmt.Sprintf("interesting %d", counter))
				counter++
			case r := <-genClosing:
				r <- nil
			}
		}
	}
	go eventGenerator()

	start := time.Now()

	// create 100 subscriptions to topic that will consume events
	subs := make([]Subscription, 100)
	for i := 0; i < len(subs); i++ {
		subs[i] = topic.Subscribe()
	}

	var counter int64 = 0
	for i := 0; i < len(subs); i++ {
		go consumeSubscription(subs[i], strconv.Itoa(i), func(events []Event, name string) {
			atomic.AddInt64(&counter, 1)
		})
	}

	finish := make(chan struct{})
	time.AfterFunc(3*time.Second, func() {
		topic.Close()
		close(finish)
	})
	<-finish

	end := time.Now()
	elapsed := end.Sub(start)
	throughput := int(float64(counter) / float64(elapsed.Milliseconds()) * 60)

	expected := 40
	if throughput < expected {
		t.Fatalf("not enough events processed: expected %d, found %d", expected, throughput)
	}
}
