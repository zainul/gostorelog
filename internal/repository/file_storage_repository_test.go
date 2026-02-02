package repository

import (
	"os"
	"testing"

	"gostorelog/internal/entity"
)

func TestFileStorageRepository_AppendAndRead(t *testing.T) {
	// Use test-data dir in project root
	wd, _ := os.Getwd()
	testDataDir := wd + "/../../test-data"
	os.RemoveAll(testDataDir) // Clean up from previous runs
	os.MkdirAll(testDataDir, 0755)
	dir := testDataDir

	// Note: Not removing dir at end to allow sanity check of generated files

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
	if err := repo.Append(record); err != nil {
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

func BenchmarkFileStorageRepository_Append(b *testing.B) {
	wd, _ := os.Getwd()
	dir := wd + "/../../test-data"
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0755)

	config := &entity.Config{
		DataDir:     dir,
		MaxFileSize: 1024 * 1024, // 1MB for benchmark
	}
	repo := NewFileStorageRepository(config)

	record := &entity.Record{
		Data:         []byte("benchmark data"),
		DataType:     entity.DataTypeBytes,
		PartitionKey: "bench-partition",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record.Offset = 0 // Reset for each
		if err := repo.Append(record); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFileStorageRepository_Read(b *testing.B) {
	wd, _ := os.Getwd()
	dir := wd + "/../../test-data"
	os.RemoveAll(dir)
	os.MkdirAll(dir, 0755)

	config := &entity.Config{
		DataDir:     dir,
		MaxFileSize: 1024 * 1024,
	}
	repo := NewFileStorageRepository(config)

	// Pre-populate with some data
	for i := 0; i < 100; i++ {
		record := &entity.Record{
			Data:         []byte("benchmark data"),
			DataType:     entity.DataTypeBytes,
			PartitionKey: "bench-partition",
		}
		if err := repo.Append(record); err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		offset := uint64(i % 100)
		if _, err := repo.Read("bench-partition", offset); err != nil {
			b.Fatal(err)
		}
	}
}