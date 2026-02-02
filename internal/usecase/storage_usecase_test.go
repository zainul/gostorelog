package usecase

import (
	"io/ioutil"
	"os"
	"testing"

	"gostorelog/internal/entity"
	"gostorelog/internal/repository"
)

func TestStorageUsecase_StoreAndRetrieve(t *testing.T) {
	// Create temp dir
	dir, err := ioutil.TempDir("", "gostorelog_usecase_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	config := &entity.Config{
		DataDir:     dir,
		MaxFileSize: 1024,
	}
	repo := repository.NewFileStorageRepository(config)
	uc := NewStorageUsecase(repo)

	// Store
	err = uc.StoreRecord("test string", entity.DataTypeString, "test-partition")
	if err != nil {
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