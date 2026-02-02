package entity

import (
	"testing"
)

func TestNewPartition(t *testing.T) {
	partition := NewPartition("test", "/tmp", 1024)
	if partition.Key != "test" {
		t.Errorf("Expected Key 'test', got %s", partition.Key)
	}
	if len(partition.Segments) != 1 {
		t.Errorf("Expected 1 segment, got %d", len(partition.Segments))
	}
	if partition.MaxFileSize != 1024 {
		t.Errorf("Expected MaxFileSize 1024, got %d", partition.MaxFileSize)
	}
	t.Logf("TestNewPartition passed: partition created with initial segment")
}

func TestGetActiveSegment(t *testing.T) {
	partition := NewPartition("test", "/tmp", 100)
	active := partition.GetActiveSegment()
	if !active.IsActive {
		t.Errorf("Expected active segment")
	}
	active.IsActive = false
	active2 := partition.GetActiveSegment()
	if active2 == active {
		t.Errorf("Expected new active segment")
	}
	t.Logf("TestGetActiveSegment passed: active segment management works")
}

func TestAppendRecord(t *testing.T) {
	partition := NewPartition("test", "/tmp", 100)
	record := &Record{PartitionKey: "test"}
	err := partition.AppendRecord(record, 50)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if record.Offset != 0 {
		t.Errorf("Expected offset 0, got %d", record.Offset)
	}
	if partition.CurrentOffset != 1 {
		t.Errorf("Expected CurrentOffset 1, got %d", partition.CurrentOffset)
	}
	// Add another to trigger rollover
	record2 := &Record{PartitionKey: "test"}
	err = partition.AppendRecord(record2, 60)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if record2.Offset != 1 {
		t.Errorf("Expected offset 1, got %d", record2.Offset)
	}
	if len(partition.Segments) != 2 {
		t.Errorf("Expected 2 segments, got %d", len(partition.Segments))
	}
	t.Logf("TestAppendRecord passed: record appended and rollover works")
}