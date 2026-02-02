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

	// Cluster configuration
	clusterConfig := cluster.DefaultConfig()
	if nodeID := os.Getenv("NODE_ID"); nodeID != "" {
		clusterConfig.NodeID = nodeID
	}
	if bindAddr := os.Getenv("BIND_ADDR"); bindAddr != "" {
		clusterConfig.BindAddr = bindAddr
	}
	if advertiseAddr := os.Getenv("ADVERTISE_ADDR"); advertiseAddr != "" {
		clusterConfig.AdvertiseAddr = advertiseAddr
	}
	if redisAddr := os.Getenv("REDIS_ADDR"); redisAddr != "" {
		clusterConfig.RedisAddr = redisAddr
	}
	if serviceName := os.Getenv("SERVICE_NAME"); serviceName != "" {
		clusterConfig.ServiceName = serviceName
	}
	if port := os.Getenv("CLUSTER_PORT"); port != "" {
		clusterConfig.ClusterPort = port
	}
	if dataDir := os.Getenv("DATA_DIR"); dataDir != "" {
		clusterConfig.DataDir = dataDir
	}

	// Initialize layers
	repo := repository.NewFileStorageRepository(config)
	uc := usecase.NewStorageUsecase(repo)
	connector := handler.NewGoPubSubConnector()
	storageHandler := handler.NewStorageHandler(uc, connector)
	httpHandler := handler.NewHTTPHandler(uc)
	server := httpHandler.StartServer(":8080")

	// Initialize cluster manager
	clusterManager, err := cluster.NewManager(clusterConfig, []string{}, uc)
	if err != nil {
		log.Fatal("Failed to create cluster manager:", err)
	}
	uc.SetReplicator(clusterManager) // Set cluster manager as replicator
	if err := clusterManager.Start(); err != nil {
		log.Fatal("Failed to start cluster:", err)
	}

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