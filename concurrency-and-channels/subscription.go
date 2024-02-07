package main

import (
	"sync"
	"time"
)

const DefaultMaxPending int = 100
const DefaultPollInterval time.Duration = 100 * time.Millisecond

// The Event type represents a single event.
// When a notification is sent to a Topic a new Event type is pushed
// into a subscriber's channel.
type Event struct {
	Content   string
	TopicName string
	ts        time.Time
}

// The EventStore type represents a buffer of updates that are sent to
// a Topic. The store will buffer the first MaxPending events. When more
// events are pushed into the store, older events will be deleted.
type EventStore struct {
	MaxPending  int
	updates     []Event
	lastUpdated time.Time
	mu          sync.RWMutex
}

// Push a new event into the store. If the store is at capacity, this operation
// will cause the updates to re-slice to eliminate older events.
func (s *EventStore) Push(evt Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	maxPending := s.MaxPending
	if maxPending <= 0 {
		maxPending = DefaultMaxPending
	}

	evt.ts = time.Now() // make sure sender is not tampering with timestamp
	u := append(s.updates, evt)
	if len(u) > maxPending {
		u = u[1:]
	}
	s.lastUpdated = evt.ts
	s.updates = u
}

// Return updates since the specified fromTime.
// This method will compare the event's timestamp with fromTime and only return events
// that are older.
//
// This is useful if we want to avoid fetching same updates multiple times.
func (s *EventStore) UpdatesSince(fromTime time.Time) []Event {
	s.mu.RLocker().Lock()
	defer s.mu.RLocker().Unlock()

	idx := -1
	for i := 0; i < len(s.updates) && idx < 0; i++ {
		if s.updates[i].ts.After(fromTime) {
			idx = i
		}
	}
	if idx < 0 {
		return nil
	}

	return s.updates[idx:]
}

// Returns true if the store has updates that are more recent than fromTime.
// This function is used to optimize the event search on the buffer to avoid
// traversing the slice when there's nothing to fetch.
func (s *EventStore) HasUpdates(fromTime time.Time) bool {
	s.mu.RLocker().Lock()
	defer s.mu.RLocker().Unlock()
	return s.lastUpdated.After(fromTime)
}

// Initializes a new instance of Topic.
func NewTopic(name string) *Topic {
	t := &Topic{Name: name}
	t.store = &EventStore{updates: []Event{}}
	t.done = make(chan struct{})
	return t
}

// The Topic structure represents a topic routines can subscribe to.
// A Topic can receive events that will be multicasted to all its subscribers.
type Topic struct {
	Name  string
	store *EventStore
	done  chan struct{}
}

// Push a new event into the topic buffer.
func (t *Topic) Push(content string) {
	evt := Event{content, t.Name, time.Now()}
	t.store.Push(evt)
}

// Close a topic.
// Calling this method will send a done signal to all the subscriber loops
// causing all channels to close. Closing a topic with the Close() method allows
// graceful termination of all event fetch loops.
func (t *Topic) Close() error {
	close(t.done)
	return nil
}

// Subscribe to Topic.
// Calling this method will effectively create a new Subscription that can be
// used to stream new events that are published in the Topic.
func (t *Topic) Subscribe() Subscription {
	stream := make(chan []Event)
	closing := make(chan chan error)

	go t.loop(stream, closing)

	return &sub{stream, closing}
}

// Internal event fetch loop.
// The loop handles three types of event:
//   - send new events from the buffer to the Topic subscriber
//   - receive closing signal from subscriber
//     Subscriber will stop consuming updates, hence closing
//   - receive done signal from Topic. Topic is no longer active, so
//     all loops will exit
//
// Note that fetchUpdates and sendUpdates have been separated into two
// cases. This is to avoid blocking the stream write while inside one of
// the select cases. Bad subscription consumers can cause the loop to block
// and become unresponsive to closing requests.
// This scenario is very possible as if a subscriber wants to close the stream
// it could no longer be consuming messages from the queue.
func (t *Topic) loop(stream chan []Event, closing chan chan error) {
	lastUpdate := time.Now()
	var updates []Event

	for {
		var sendUpdates chan<- []Event
		var fetchUpdates <-chan time.Time
		if len(updates) > 0 {
			// enable send to stream
			sendUpdates = stream
		} else {
			// enable fetch new updates
			fetchUpdates = time.After(DefaultPollInterval)
		}

		select {
		case <-t.done:
			close(stream)
			return
		case errc := <-closing:
			close(stream)
			errc <- nil
			return
		case <-fetchUpdates:
			if !t.store.HasUpdates(lastUpdate) {
				break
			}
			updates = t.store.UpdatesSince(lastUpdate)
		case sendUpdates <- updates:
			last := updates[len(updates)-1]
			lastUpdate = last.ts
			updates = []Event{}
		}
	}
}

// The Subscription interface defines how we can interact with a topic Subscription.
type Subscription interface {
	Updates() <-chan []Event
	Close() error
}

// The sub struct is an internal type that holds the state of a topic Subscription.
type sub struct {
	stream  chan []Event
	closing chan chan error
}

// Returns the channel used to receive Event updates.
// Every subscriber should consume updates using a structure like:
//
//	for e := <- sub.Updates() {
//	    ...
//	}
func (s *sub) Updates() <-chan []Event {
	return s.stream
}

// Close the subscription.
// The close function sends a closing request to the Topic, which will respond
// using the enclosed channel if any error has to be notified. Calling close
// will allow graceful termination of in-flight updates.
func (s *sub) Close() error {
	errch := make(chan error)
	s.closing <- errch
	return <-errch
}
