package cluster

import (
	"net"
	"testing"

	"gostorelog/internal/entity"
	"gostorelog/internal/repository"
	"gostorelog/internal/usecase"
)

// isPortAvailable checks if a port is available for binding
func isPortAvailable(port string) bool {
	ln, err := net.Listen("tcp", "localhost:"+port)
	if err != nil {
		return false
	}
	ln.Close()
	return true
}

func TestManager_NewManager(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"

	// Create a dummy usecase
	repo := repository.NewFileStorageRepository(&entity.Config{DataDir: "/tmp", MaxFileSize: 1024})
	uc := usecase.NewStorageUsecase(repo)

	// Check if port is available
	if !isPortAvailable("7946") {
		t.Skipf("Port 7946 is already in use, skipping test")
	}

	t.Logf("Scenario: Creating cluster manager for node %s", config.NodeID)
	t.Logf("Input: Config=%+v", config)

	manager, err := NewManager(config, []string{}, uc)
	t.Logf("Output: NewManager returned manager=%v, error=%v", manager != nil, err)

	if err != nil {
		t.Logf("Result: Manager creation failed: %v", err)
	} else {
		t.Logf("Result: Manager created successfully")
		manager.Shutdown()
	}
}

func TestManager_IsLeader(t *testing.T) {
	config := DefaultConfig()
	config.NodeID = "test-node"

	// Create a dummy usecase
	repo := repository.NewFileStorageRepository(&entity.Config{DataDir: "/tmp", MaxFileSize: 1024})
	uc := usecase.NewStorageUsecase(repo)

	// Check if port is available
	if !isPortAvailable("7946") {
		t.Skipf("Port 7946 is already in use, skipping test")
	}

	manager, err := NewManager(config, []string{}, uc)
	if err != nil {
		t.Logf("Manager creation failed: %v", err)
		return
	}
	defer manager.Shutdown()

	t.Logf("Scenario: Checking if node is leader initially")
	isLeader := manager.IsLeader()
	t.Logf("Output: IsLeader returned %v", isLeader)
	t.Logf("Result: Node is %s", map[bool]string{true: "leader", false: "follower"}[isLeader])
}