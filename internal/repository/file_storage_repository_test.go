package repository

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"gostorelog/internal/entity"
)

func TestFileStorageRepository_AppendAndRead(t *testing.T) {
	// Use test-data dir in project root
	wd, _ := os.Getwd()
	testDataDir := wd + "/../../test-data/repository_test"
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
	dir := wd + "/../../test-data/benchmark_append"
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
	dir := wd + "/../../test-data/benchmark_read"
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

func TestFileStorageRepository_Segmentation(t *testing.T) {
	// Use test-data dir in project root
	wd, _ := os.Getwd()
	testDataDir := wd + "/../../test-data/segmentation_test"
	os.RemoveAll(testDataDir) // Clean up from previous runs
	os.MkdirAll(testDataDir, 0755)
	dir := testDataDir

	config := &entity.Config{
		DataDir:     dir,
		MaxFileSize: 10, // Small size to trigger segmentation
	}
	repo := NewFileStorageRepository(config)

	partitionKey := "segment-test-partition"

	// Append 4 records, each should trigger a new segment
	for i := 0; i < 4; i++ {
		record := &entity.Record{
			Data:         []byte("x"), // 1 byte data
			DataType:     entity.DataTypeBytes,
			PartitionKey: partitionKey,
		}
		if err := repo.Append(record); err != nil {
			t.Fatalf("Append %d failed: %v", i, err)
		}
	}

	// Check that 4 segments are created
	files, err := os.ReadDir(dir + "/" + partitionKey)
	if err != nil {
		t.Fatal(err)
	}
	storeCount := 0
	for _, file := range files {
		if strings.HasSuffix(file.Name(), ".store") {
			storeCount++
		}
	}
	if storeCount != 4 {
		t.Errorf("Expected 4 segments, got %d", storeCount)
	}

	// Dump files to txt for readability
	dumpFilesToTxt(dir + "/" + partitionKey)

	t.Logf("Segmentation test passed: %d segments created", storeCount)
}

// dumpFilesToTxt reads binary .store and .index files and writes human-readable .txt versions
func dumpFilesToTxt(partitionDir string) {
	files, err := os.ReadDir(partitionDir)
	if err != nil {
		return
	}
	for _, file := range files {
		name := file.Name()
		if strings.HasSuffix(name, ".store") {
			dumpStoreToTxt(partitionDir + "/" + name)
		} else if strings.HasSuffix(name, ".index") {
			dumpIndexToTxt(partitionDir + "/" + name)
		}
	}
}

func dumpStoreToTxt(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()

	txtFilename := filename + ".txt"
	txtFile, err := os.Create(txtFilename)
	if err != nil {
		return
	}
	defer txtFile.Close()

	pos := int64(0)
	for {
		var length uint32
		if err := binary.Read(file, binary.BigEndian, &length); err != nil {
			break
		}
		var dataType byte
		if err := binary.Read(file, binary.BigEndian, &dataType); err != nil {
			break
		}
		data := make([]byte, length-1)
		if _, err := io.ReadFull(file, data); err != nil {
			break
		}
		fmt.Fprintf(txtFile, "Pos: %d, Type: %d, Data: %s\n", pos, dataType, string(data))
		pos += 4 + int64(length)
		file.Seek(pos, 0)
	}
}

func dumpIndexToTxt(filename string) {
	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()

	txtFilename := filename + ".txt"
	txtFile, err := os.Create(txtFilename)
	if err != nil {
		return
	}
	defer txtFile.Close()

	for {
		var offset, position uint64
		if err := binary.Read(file, binary.BigEndian, &offset); err != nil {
			break
		}
		if err := binary.Read(file, binary.BigEndian, &position); err != nil {
			break
		}
		fmt.Fprintf(txtFile, "Offset: %d, Position: %d\n", offset, position)
	}
}