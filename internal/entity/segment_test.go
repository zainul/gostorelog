package entity

import (
	"testing"
)

func TestNewSegment(t *testing.T) {
	segment := NewSegment("test-partition", 100, 1024*1024, "/tmp")
	if segment.BaseOffset != 100 {
		t.Errorf("Expected BaseOffset 100, got %d", segment.BaseOffset)
	}
	if segment.NextOffset != 100 {
		t.Errorf("Expected NextOffset 100, got %d", segment.NextOffset)
	}
	if segment.MaxSize != 1024*1024 {
		t.Errorf("Expected MaxSize 1048576, got %d", segment.MaxSize)
	}
	if !segment.IsActive {
		t.Errorf("Expected IsActive true")
	}
	t.Logf("TestNewSegment passed: segment created correctly")
}

func TestShouldRollOver(t *testing.T) {
	segment := NewSegment("test", 0, 100, "/tmp")
	segment.Size = 90
	if !segment.ShouldRollOver(15) {
		t.Errorf("Expected to roll over")
	}
	if segment.ShouldRollOver(5) {
		t.Errorf("Expected not to roll over")
	}
	t.Logf("TestShouldRollOver passed: roll over logic works")
}

func TestAddRecord(t *testing.T) {
	segment := NewSegment("test", 0, 100, "/tmp")
	segment.AddRecord(10)
	if segment.Size != 10 {
		t.Errorf("Expected Size 10, got %d", segment.Size)
	}
	if segment.NextOffset != 1 {
		t.Errorf("Expected NextOffset 1, got %d", segment.NextOffset)
	}
	t.Logf("TestAddRecord passed: record added correctly")
}