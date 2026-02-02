package handler

import "context"

// Message represents a message in the pub/sub system
type Message struct {
	Key   string
	Value []byte
}

// Connector defines the interface for pub/sub connectors
type Connector interface {
	// Publish publishes a message to a topic
	Publish(ctx context.Context, topic string, message *Message) error
	// Subscribe subscribes to a topic and calls the handler for each message
	Subscribe(ctx context.Context, topic string, handler func(*Message) error) error
	// Close closes the connector
	Close() error
}