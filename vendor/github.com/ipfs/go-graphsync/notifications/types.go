package notifications

// Topic is a topic that events appear on
type Topic interface{}

// Event is a publishable event
type Event interface{}

// TopicData is data added to every message broadcast on a topic
type TopicData interface{}

// Subscriber is a subscriber that can receive events
type Subscriber interface {
	OnNext(Topic, Event)
	OnClose(Topic)
}

// Subscribable is a stream that can be subscribed to
type Subscribable interface {
	Subscribe(topic Topic, sub Subscriber) bool
	Unsubscribe(sub Subscriber) bool
}

// Publisher is an publisher of events that can be subscribed to
type Publisher interface {
	Close(Topic)
	Publish(Topic, Event)
	Shutdown()
	Startup()
	Subscribable
}
