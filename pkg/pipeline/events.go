package pipeline

import (
	"sync"
	"time"
)

// EventType identifies the kind of pipeline event emitted to SSE subscribers.
type EventType string

const (
	EventJobQueued       EventType = "job_queued"
	EventJobProcessing   EventType = "job_processing"
	EventJobCompleted    EventType = "job_completed"
	EventJobDeadLettered EventType = "job_dead_lettered"
	EventBackpressure    EventType = "backpressure"
	EventPoolStats       EventType = "pool_stats"
)

// Event is a single pipeline lifecycle event, serialised to SSE clients.
type Event struct {
	Type      EventType      `json:"type"`
	Timestamp time.Time      `json:"timestamp"`
	Data      map[string]any `json:"data"`
}

// EventBus is a thread-safe publish/subscribe hub for pipeline events.
// Subscribers receive events on a buffered channel. If a subscriber's
// channel is full, the event is dropped (non-blocking publish) to avoid
// stalling the pipeline hot path.
type EventBus struct {
	mu          sync.RWMutex
	subscribers map[int]chan Event
	nextID      int
}

// NewEventBus creates an EventBus ready for subscriptions.
func NewEventBus() *EventBus {
	return &EventBus{
		subscribers: make(map[int]chan Event),
	}
}

// Subscribe registers a new subscriber and returns a unique ID and a
// read-only event channel. The channel is buffered to bufSize to absorb
// short bursts without dropping events.
func (eb *EventBus) Subscribe(bufSize int) (int, <-chan Event) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	id := eb.nextID
	eb.nextID++
	ch := make(chan Event, bufSize)
	eb.subscribers[id] = ch
	return id, ch
}

// Unsubscribe removes a subscriber and closes its channel.
func (eb *EventBus) Unsubscribe(id int) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	if ch, ok := eb.subscribers[id]; ok {
		close(ch)
		delete(eb.subscribers, id)
	}
}

// Publish sends an event to all subscribers. If a subscriber's channel
// is full, the event is silently dropped for that subscriber.
func (eb *EventBus) Publish(evt Event) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	for _, ch := range eb.subscribers {
		select {
		case ch <- evt:
		default:
			// Drop — subscriber is too slow; don't stall the pipeline.
		}
	}
}
