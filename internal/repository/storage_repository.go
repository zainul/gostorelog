package repository

import "gostorelog/internal/entity"

// StorageRepository defines the interface for storage operations
type StorageRepository interface {
	// Append appends a record to the storage
	Append(record *entity.Record) error
	// Read reads a record by offset
	Read(partitionKey string, offset uint64) (*entity.Record, error)
	// Close closes the repository
	Close() error
}