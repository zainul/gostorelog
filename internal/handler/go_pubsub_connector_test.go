package handler

import (
	"context"
	"testing"
	"time"
)

func TestGoPubSubConnector_PublishSubscribe(t *testing.T) {
	connector := NewGoPubSubConnector()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	topic := "test-topic"
	messages := []*Message{}

	handler := func(msg *Message) error {
		messages = append(messages, msg)
		return nil
	}

	err := connector.Subscribe(ctx, topic, handler)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Publish
	msg := &Message{Key: "key1", Value: []byte("value1")}
	err = connector.Publish(ctx, topic, msg)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	if len(messages) != 1 {
		t.Errorf("Expected 1 message, got %d", len(messages))
	}
	if messages[0].Key != "key1" {
		t.Errorf("Expected key 'key1', got %s", messages[0].Key)
	}
	t.Logf("TestGoPubSubConnector_PublishSubscribe passed: publish and subscribe work correctly")

	// Cancel context to stop goroutine
	cancel()
	time.Sleep(10 * time.Millisecond)
	connector.Close()
}