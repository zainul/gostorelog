package entity

import (
	"testing"
)

func TestNewRecord_JSON(t *testing.T) {
	data := map[string]string{"key": "value"}
	record, err := NewRecord(data, DataTypeJSON, "test-partition")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if record.DataType != DataTypeJSON {
		t.Errorf("Expected DataTypeJSON, got %v", record.DataType)
	}
	if record.PartitionKey != "test-partition" {
		t.Errorf("Expected 'test-partition', got %s", record.PartitionKey)
	}
	recovered, err := record.GetData()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if m, ok := recovered.(map[string]interface{}); !ok || m["key"] != "value" {
		t.Errorf("Expected recovered data to be correct")
	}
	t.Logf("TestNewRecord_JSON passed: record created and data recovered correctly")
}

func TestNewRecord_Bytes(t *testing.T) {
	data := []byte("test data")
	record, err := NewRecord(data, DataTypeBytes, "test-partition")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if record.DataType != DataTypeBytes {
		t.Errorf("Expected DataTypeBytes, got %v", record.DataType)
	}
	recovered, err := record.GetData()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if b, ok := recovered.([]byte); !ok || string(b) != "test data" {
		t.Errorf("Expected recovered data to be correct")
	}
	t.Logf("TestNewRecord_Bytes passed: record created and data recovered correctly")
}

func TestNewRecord_String(t *testing.T) {
	data := "test string"
	record, err := NewRecord(data, DataTypeString, "test-partition")
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if record.DataType != DataTypeString {
		t.Errorf("Expected DataTypeString, got %v", record.DataType)
	}
	recovered, err := record.GetData()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if s, ok := recovered.(string); !ok || s != "test string" {
		t.Errorf("Expected recovered data to be correct")
	}
	t.Logf("TestNewRecord_String passed: record created and data recovered correctly")
}

func TestNewRecord_InvalidJSON(t *testing.T) {
	data := make(chan int) // Invalid JSON
	_, err := NewRecord(data, DataTypeJSON, "test-partition")
	if err == nil {
		t.Fatalf("Expected error for invalid JSON")
	}
	t.Logf("TestNewRecord_InvalidJSON passed: error returned for invalid JSON")
}