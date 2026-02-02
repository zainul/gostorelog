package handler

import (
	"context"
	"encoding/json"
	"log"

	"gostorelog/internal/entity"
	"gostorelog/internal/usecase"
)

// StorageHandler handles storage operations via pub/sub
type StorageHandler struct {
	usecase   usecase.StorageUsecase
	connector Connector
}

// NewStorageHandler creates a new storage handler
func NewStorageHandler(usecase usecase.StorageUsecase, connector Connector) *StorageHandler {
	return &StorageHandler{
		usecase:   usecase,
		connector: connector,
	}
}

// Start starts the handler
func (h *StorageHandler) Start(ctx context.Context, topic string) error {
	return h.connector.Subscribe(ctx, topic, h.handleMessage)
}

// handleMessage handles incoming messages
func (h *StorageHandler) handleMessage(msg *Message) error {
	// Assume message value is JSON with data, type, partitionKey
	var payload struct {
		Data         interface{}     `json:"data"`
		DataType     entity.DataType `json:"data_type"`
		PartitionKey string          `json:"partition_key"`
	}
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		log.Printf("Failed to unmarshal message: %v", err)
		return err
	}
	if err := h.usecase.StoreRecord(payload.Data, payload.DataType, payload.PartitionKey); err != nil {
		log.Printf("Failed to store record: %v", err)
		return err
	}
	log.Printf("Stored record for partition %s", payload.PartitionKey)
	return nil
}