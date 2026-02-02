package handler

import (
	"context"
	"sync"
)

// GoPubSubConnector implements Connector using Go channels
type GoPubSubConnector struct {
	topics map[string][]chan *Message
	mu     sync.RWMutex
}

// NewGoPubSubConnector creates a new Go pub/sub connector
func NewGoPubSubConnector() *GoPubSubConnector {
	return &GoPubSubConnector{
		topics: make(map[string][]chan *Message),
	}
}

// Publish publishes a message to a topic
func (c *GoPubSubConnector) Publish(ctx context.Context, topic string, message *Message) error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	chans, exists := c.topics[topic]
	if !exists {
		return nil // No subscribers
	}
	for _, ch := range chans {
		select {
		case ch <- message:
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Drop message if channel is full
		}
	}
	return nil
}

// Subscribe subscribes to a topic
func (c *GoPubSubConnector) Subscribe(ctx context.Context, topic string, handler func(*Message) error) error {
	c.mu.Lock()
	if _, exists := c.topics[topic]; !exists {
		c.topics[topic] = []chan *Message{}
	}
	ch := make(chan *Message, 100) // Buffered channel
	c.topics[topic] = append(c.topics[topic], ch)
	c.mu.Unlock()

	go func() {
		defer func() {
			c.mu.Lock()
			// Remove channel from topics
			if chans, exists := c.topics[topic]; exists {
				for i, ch2 := range chans {
					if ch2 == ch {
						c.topics[topic] = append(chans[:i], chans[i+1:]...)
						break
					}
				}
			}
			c.mu.Unlock()
			// Close channel only if not already closed
			defer func() {
				if r := recover(); r != nil {
					// Channel already closed
				}
			}()
			close(ch)
		}()
		for {
			select {
			case msg := <-ch:
				if err := handler(msg); err != nil {
					// Log error
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

// Close closes the connector
func (c *GoPubSubConnector) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, chans := range c.topics {
		for _, ch := range chans {
			close(ch)
		}
	}
	c.topics = nil
	return nil
}