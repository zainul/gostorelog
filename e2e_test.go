package main

import (
	"io/ioutil"
	"log"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"gostorelog/internal/entity"
	"gostorelog/internal/handler"
	"gostorelog/internal/repository"
	"gostorelog/internal/usecase"
	"gostorelog/pkg/client"
)

func setupServer(t *testing.T, cleanupData bool) (*httptest.Server, *client.Client, func()) {
	// Temp dir
	dir, err := ioutil.TempDir("", "e2e_test")
	if err != nil {
		t.Fatal(err)
	}

	config := &entity.Config{
		DataDir:     dir,
		MaxFileSize: 1024,
	}
	repo := repository.NewFileStorageRepository(config)
	uc := usecase.NewStorageUsecase(repo)
	httpHandler := handler.NewHTTPHandler(uc)

	// Use httptest.Server for testing
	server := httptest.NewServer(httpHandler.GetMux())
	client := client.NewClient(server.URL)

	cleanup := func() {
		server.Close()
		repo.Close()
		if cleanupData {
			os.RemoveAll(dir)
		}
	}

	return server, client, cleanup
}

func TestEndToEnd_PublishAndRead(t *testing.T) {
	_, c, cleanup := setupServer(t, false)
	defer cleanup()

	data := map[string]string{"key": "value"}
	err := c.Publish(data, int(entity.DataTypeJSON), "test-partition")
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	record, err := c.Read("test-partition", 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}

	if record.Offset != 0 {
		t.Errorf("Expected offset 0, got %d", record.Offset)
	}
	if record.PartitionKey != "test-partition" {
		t.Errorf("Expected partition 'test-partition', got %s", record.PartitionKey)
	}
	t.Logf("TestEndToEnd_PublishAndRead passed: published and read record successfully")
}

func TestEndToEnd_SequentialOffsets(t *testing.T) {
	_, c, cleanup := setupServer(t, false)
	defer cleanup()

	for i := 0; i < 3; i++ {
		data := map[string]int{"id": i}
		err := c.Publish(data, int(entity.DataTypeJSON), "test-partition")
		if err != nil {
			t.Fatalf("Publish %d failed: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond) // Allow processing
	}

	for i := 0; i < 3; i++ {
		record, err := c.Read("test-partition", uint64(i))
		if err != nil {
			t.Fatalf("Read %d failed: %v", i, err)
		}
		if record.Offset != uint64(i) {
			t.Errorf("Expected offset %d, got %d", i, record.Offset)
		}
	}
	t.Logf("TestEndToEnd_SequentialOffsets passed: offsets are sequential")
}

func TestEndToEnd_RestartAndRecovery(t *testing.T) {
	dir, err := ioutil.TempDir("", "e2e_restart_test")
	if err != nil {
		t.Fatal(err)
	}
	cleanupData := false // Keep for verification
	if cleanupData {
		defer os.RemoveAll(dir)
	}

	config := &entity.Config{
		DataDir:     dir,
		MaxFileSize: 1024,
	}

	// First server instance
	repo1 := repository.NewFileStorageRepository(config)
	uc1 := usecase.NewStorageUsecase(repo1)
	httpHandler1 := handler.NewHTTPHandler(uc1)
	server1 := httptest.NewServer(httpHandler1.GetMux())
	client1 := client.NewClient(server1.URL)

	// Publish some data
	data1 := "data1"
	err = client1.Publish(data1, int(entity.DataTypeString), "partition1")
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}
	data2 := "data2"
	err = client1.Publish(data2, int(entity.DataTypeString), "partition1")
	if err != nil {
		t.Fatalf("Publish failed: %v", err)
	}

	// Shutdown first server
	server1.Close()
	repo1.Close()
	log.Println("First server shut down")

	// Start second server instance (simulating restart)
	repo2 := repository.NewFileStorageRepository(config)
	uc2 := usecase.NewStorageUsecase(repo2)
	httpHandler2 := handler.NewHTTPHandler(uc2)
	server2 := httptest.NewServer(httpHandler2.GetMux())
	client2 := client.NewClient(server2.URL)

	// Read the data back
	record1, err := client2.Read("partition1", 0)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if record1.Offset != 0 {
		t.Errorf("Expected offset 0, got %d", record1.Offset)
	}
	recoveredData1, _ := record1.GetData()
	if recoveredData1 != "data1" {
		t.Errorf("Expected 'data1', got %v", recoveredData1)
	}

	record2, err := client2.Read("partition1", 1)
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if record2.Offset != 1 {
		t.Errorf("Expected offset 1, got %d", record2.Offset)
	}
	recoveredData2, _ := record2.GetData()
	if recoveredData2 != "data2" {
		t.Errorf("Expected 'data2', got %v", recoveredData2)
	}

	// Shutdown second server
	server2.Close()
	repo2.Close()

	if cleanupData {
		os.RemoveAll(dir)
	}

	t.Logf("TestEndToEnd_RestartAndRecovery passed: data persisted across restart")
}