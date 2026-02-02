package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	httpHandler := handler.NewHTTPHandler(uc)
	server := httpHandler.StartServer(":8080")

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

	// Graceful shutdown
	log.Println("Shutting down server...")
	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()

	if err := server.Shutdown(ctxShutdown); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}

	connector.Close()
	repo.Close()
	log.Println("Shutdown complete")
}