package repository

import (
	"io/ioutil"
	"os"
	"testing"

	"gostorelog/internal/entity"
)

func TestFileStorageRepository_AppendAndRead(t *testing.T) {
	// Create temp dir
	dir, err := ioutil.TempDir("", "gostorelog_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := &entity.Config{
		DataDir:     dir,
		MaxFileSize: 1024,
	}
	repo := NewFileStorageRepository(config)

	// Create record
	record := &entity.Record{
		Data:         []byte("test data"),
		DataType:     entity.DataTypeBytes,
		PartitionKey: "test-partition",
	}

	// Append
	err = repo.Append(record)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if record.Offset != 0 {
		t.Errorf("Expected offset 0, got %d", record.Offset)
	}

	// Read
	readRecord, err := repo.Read("test-partition", 0)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if string(readRecord.Data) != "test data" {
		t.Errorf("Expected 'test data', got %s", string(readRecord.Data))
	}
	if readRecord.DataType != entity.DataTypeBytes {
		t.Errorf("Expected DataTypeBytes, got %v", readRecord.DataType)
	}
	t.Logf("TestFileStorageRepository_AppendAndRead passed: append and read work correctly")
}