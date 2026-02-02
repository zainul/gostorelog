package repository

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"gostorelog/internal/entity"
)

// FileStorageRepository implements StorageRepository using file system
type FileStorageRepository struct {
	config     *entity.Config
	partitions map[string]*entity.Partition
	mu         sync.RWMutex
}

// NewFileStorageRepository creates a new file storage repository
func NewFileStorageRepository(config *entity.Config) *FileStorageRepository {
	repo := &FileStorageRepository{
		config:     config,
		partitions: make(map[string]*entity.Partition),
	}
	// Load existing partitions and segments
	repo.loadExistingData()
	return repo
}

// loadExistingData loads existing partitions and segments from disk
func (r *FileStorageRepository) loadExistingData() {
	// List partition directories
	entries, err := os.ReadDir(r.config.DataDir)
	if err != nil {
		// Directory doesn't exist or error, create it
		os.MkdirAll(r.config.DataDir, 0755)
		return
	}
	for _, entry := range entries {
		if entry.IsDir() {
			partitionKey := entry.Name()
			r.loadPartition(partitionKey)
		}
	}
}

// loadPartition loads a partition from disk
func (r *FileStorageRepository) loadPartition(partitionKey string) {
	partitionDir := filepath.Join(r.config.DataDir, partitionKey)
	partition := entity.NewPartition(partitionKey, r.config.DataDir, r.config.MaxFileSize)
	// Load segments
	entries, err := os.ReadDir(partitionDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() && filepath.Ext(entry.Name()) == ".store" {
			// Load segment
			segmentPath := filepath.Join(partitionDir, entry.Name())
			baseOffsetStr := entry.Name()[:len(entry.Name())-6] // remove .store
			var baseOffset uint64
			fmt.Sscanf(baseOffsetStr, "segment_%d", &baseOffset)
			segment := entity.NewSegment(partitionKey, baseOffset, r.config.MaxFileSize, r.config.DataDir)
			// Calculate size and next offset
			if stat, err := os.Stat(segmentPath); err == nil {
				segment.Size = uint64(stat.Size())
			}
			// Load index to get next offset
			indexPath := filepath.Join(partitionDir, baseOffsetStr+".index")
			if file, err := os.Open(indexPath); err == nil {
				defer file.Close()
				scanner := bufio.NewScanner(file)
				count := uint64(0)
				for scanner.Scan() {
					count++
				}
				segment.NextOffset = baseOffset + count
			}
			partition.Segments = append(partition.Segments, segment)
			if segment.NextOffset > partition.CurrentOffset {
				partition.CurrentOffset = segment.NextOffset
			}
		}
	}
	r.partitions[partitionKey] = partition
}

// Append appends a record to the storage
func (r *FileStorageRepository) Append(record *entity.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	partition, exists := r.partitions[record.PartitionKey]
	if !exists {
		partition = entity.NewPartition(record.PartitionKey, r.config.DataDir, r.config.MaxFileSize)
		r.partitions[record.PartitionKey] = partition
		// Create partition dir
		os.MkdirAll(filepath.Join(r.config.DataDir, record.PartitionKey), 0755)
	}

	activeSegment := partition.GetActiveSegment()

	// Calculate record size (data + metadata)
	recordSize := uint64(len(record.Data) + 16) // rough estimate

	if activeSegment.ShouldRollOver(recordSize) {
		activeSegment.IsActive = false
		partition.Segments = append(partition.Segments, entity.NewSegment(record.PartitionKey, partition.CurrentOffset, r.config.MaxFileSize, r.config.DataDir))
		activeSegment = partition.Segments[len(partition.Segments)-1]
	}

	record.Offset = activeSegment.NextOffset

	// Write to .store file
	storeFile, err := os.OpenFile(activeSegment.StorePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer storeFile.Close()

	// Get current position
	stat, err := storeFile.Stat()
	if err != nil {
		return err
	}
	position := stat.Size()

	// Write record: [length 4][dataType 1][data]
	length := uint32(len(record.Data) + 1) // +1 for dataType
	if err := binary.Write(storeFile, binary.BigEndian, length); err != nil {
		return err
	}
	dataTypeByte := byte(record.DataType)
	if _, err := storeFile.Write([]byte{dataTypeByte}); err != nil {
		return err
	}
	if _, err := storeFile.Write(record.Data); err != nil {
		return err
	}

	// Write to .index file: offset and position
	indexFile, err := os.OpenFile(activeSegment.IndexPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	if err := binary.Write(indexFile, binary.BigEndian, record.Offset); err != nil {
		return err
	}
	if err := binary.Write(indexFile, binary.BigEndian, uint64(position)); err != nil {
		return err
	}

	// Update segment
	activeSegment.AddRecord(recordSize)
	partition.CurrentOffset = activeSegment.NextOffset

	return nil
}

// Read reads a record by offset
func (r *FileStorageRepository) Read(partitionKey string, offset uint64) (*entity.Record, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	partition, exists := r.partitions[partitionKey]
	if !exists {
		return nil, errors.New("partition not found")
	}

	// Find the segment containing the offset
	var targetSegment *entity.Segment
	for _, seg := range partition.Segments {
		if offset >= seg.BaseOffset && offset < seg.NextOffset {
			targetSegment = seg
			break
		}
	}
	if targetSegment == nil {
		return nil, errors.New("offset not found")
	}

	// Open index file to find position
	indexFile, err := os.Open(targetSegment.IndexPath)
	if err != nil {
		return nil, err
	}
	defer indexFile.Close()

	relativeOffset := offset - targetSegment.BaseOffset
	// Each entry is 16 bytes (offset 8, position 8)
	indexFile.Seek(int64(relativeOffset*16), 0)

	var storedOffset, position uint64
	if err := binary.Read(indexFile, binary.BigEndian, &storedOffset); err != nil {
		return nil, err
	}
	if err := binary.Read(indexFile, binary.BigEndian, &position); err != nil {
		return nil, err
	}

	// Open store file and read the record
	storeFile, err := os.Open(targetSegment.StorePath)
	if err != nil {
		return nil, err
	}
	defer storeFile.Close()

	storeFile.Seek(int64(position), 0)
	var length uint32
	if err := binary.Read(storeFile, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	// Read dataType and data
	dataTypeByte := make([]byte, 1)
	if _, err := io.ReadFull(storeFile, dataTypeByte); err != nil {
		return nil, err
	}
	data := make([]byte, length-1)
	if _, err := io.ReadFull(storeFile, data); err != nil {
		return nil, err
	}

	record := &entity.Record{
		Offset:       offset,
		Data:         data,
		DataType:     entity.DataType(dataTypeByte[0]),
		PartitionKey: partitionKey,
	}

	return record, nil
}

// Close closes the repository
func (r *FileStorageRepository) Close() error {
	// Nothing to close for file repo
	return nil
}