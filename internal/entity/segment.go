package entity

import (
	"fmt"
	"path/filepath"
)

// Segment represents a segment file containing records
type Segment struct {
	PartitionKey string `json:"partition_key"`
	BaseOffset   uint64 `json:"base_offset"` // Starting offset of this segment
	NextOffset   uint64 `json:"next_offset"` // Next offset to assign
	Size         uint64 `json:"size"`        // Current size in bytes
	MaxSize      uint64 `json:"max_size"`    // Max size before rolling over
	StorePath    string `json:"store_path"`  // Path to .store file
	IndexPath    string `json:"index_path"`  // Path to .index file
	IsActive     bool   `json:"is_active"`   // Whether this segment is active for writing
}

// NewSegment creates a new segment for a partition
func NewSegment(partitionKey string, baseOffset uint64, maxSize uint64, dataDir string) *Segment {
	storePath := filepath.Join(dataDir, partitionKey, fmt.Sprintf("segment_%d.store", baseOffset))
	indexPath := filepath.Join(dataDir, partitionKey, fmt.Sprintf("segment_%d.index", baseOffset))
	return &Segment{
		PartitionKey: partitionKey,
		BaseOffset:   baseOffset,
		NextOffset:   baseOffset,
		Size:         0,
		MaxSize:      maxSize,
		StorePath:    storePath,
		IndexPath:    indexPath,
		IsActive:     true,
	}
}

// ShouldRollOver checks if the segment should roll over to a new one
func (s *Segment) ShouldRollOver(recordSize uint64) bool {
	return s.Size+recordSize > s.MaxSize
}

// AddRecord updates the segment state after adding a record
func (s *Segment) AddRecord(recordSize uint64) {
	s.Size += recordSize
	s.NextOffset++
}