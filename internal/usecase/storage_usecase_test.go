package usecase

import (
	"os"
	"testing"

	"gostorelog/internal/entity"
	"gostorelog/internal/repository"
)

func TestStorageUsecase_StoreAndRetrieve(t *testing.T) {
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
	repo := repository.NewFileStorageRepository(config)
	uc := NewStorageUsecase(repo)

	// Store
	if err := uc.StoreRecord("test string", entity.DataTypeString, "test-partition"); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Retrieve
	record, err := uc.RetrieveRecord("test-partition", 0)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	data, err := record.GetData()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	if s, ok := data.(string); !ok || s != "test string" {
		t.Errorf("Expected 'test string', got %v", data)
	}
	t.Logf("TestStorageUsecase_StoreAndRetrieve passed: store and retrieve work correctly")
}