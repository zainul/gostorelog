package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"gostorelog/internal/entity"
	"gostorelog/internal/handler"
	"gostorelog/internal/repository"
	"gostorelog/internal/usecase"
)

func main() {
	// Configuration
	config := &entity.Config{
		DataDir:     "./data",
		MaxFileSize: 10 * 1024 * 1024, // 10MB
	}

	// Initialize layers
	repo := repository.NewFileStorageRepository(config)
	uc := usecase.NewStorageUsecase(repo)
	connector := handler.NewGoPubSubConnector()
	storageHandler := handler.NewStorageHandler(uc, connector)

	// Start handler
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := "storage-topic"
	if err := storageHandler.Start(ctx, topic); err != nil {
		log.Fatal(err)
	}

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// Close
	connector.Close()
	repo.Close()
	log.Println("Shutdown complete")
}