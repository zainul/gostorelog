package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gostorelog/internal/cluster"
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

	// Cluster configuration (example values)
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "node1"
	}
	bindAddr := os.Getenv("BIND_ADDR")
	if bindAddr == "" {
		bindAddr = "0.0.0.0:7946"
	}
	advertiseAddr := os.Getenv("ADVERTISE_ADDR")
	if advertiseAddr == "" {
		advertiseAddr = "127.0.0.1:7946"
	}
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}
	serviceName := os.Getenv("SERVICE_NAME")
	if serviceName == "" {
		serviceName = "gostorelog-cluster"
	}
	port := os.Getenv("CLUSTER_PORT")
	if port == "" {
		port = "7946"
	}

	// Initialize cluster manager
	clusterManager, err := cluster.NewManager(nodeID, bindAddr, advertiseAddr, redisAddr, serviceName, port)
	if err != nil {
		log.Fatal("Failed to create cluster manager:", err)
	}
	if err := clusterManager.Start(); err != nil {
		log.Fatal("Failed to start cluster:", err)
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
	clusterManager.Shutdown()
	log.Println("Shutdown complete")
}