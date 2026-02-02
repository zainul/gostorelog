package cluster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/hashicorp/memberlist"
	"gostorelog/internal/entity"
	"gostorelog/internal/usecase"
)

// Manager manages the cluster
type Manager struct {
	config         *Config
	leaderElection *LeaderElection
	gossip         *Gossip
	dnsResolver    *DNSResolver
	usecase        usecase.StorageUsecase
	isLeader       bool
}

// NewManager creates a new cluster manager
func NewManager(config *Config, peers []string, uc usecase.StorageUsecase) (*Manager, error) {
	le, err := NewLeaderElection(config, peers)
	if err != nil {
		return nil, err
	}
	gossip, err := NewGossip(config)
	if err != nil {
		return nil, err
	}
	dns := NewDNSResolver(config)

	log.Printf("Creating cluster manager for node %s", config.NodeID)
	return &Manager{
		config:         config,
		leaderElection: le,
		gossip:         gossip,
		dnsResolver:    dns,
		usecase:        uc,
	}, nil
}

// Start starts the cluster manager
func (m *Manager) Start() error {
	log.Printf("Starting cluster manager for node %s", m.config.NodeID)
	// Try to become leader
	if m.leaderElection.TryBecomeLeader() {
		m.isLeader = true
		log.Printf("Node %s started as leader", m.config.NodeID)
		// As leader, start DNS watching to discover nodes
		go m.dnsResolver.WatchNodes(m.config.WatchInterval, func(nodes []string) {
			log.Printf("Leader %s discovered nodes via DNS: %v", m.config.NodeID, nodes)
			// Join the gossip cluster
			err := m.gossip.Join(nodes)
			if err != nil {
				log.Printf("Leader %s failed to join gossip: %v", m.config.NodeID, err)
			}
		})
	} else {
		log.Printf("Node %s started as follower", m.config.NodeID)
		// As follower, periodically try to become leader
		go m.tryBecomeLeaderPeriodically()
		// Discover initial nodes via DNS
		nodes, err := m.dnsResolver.ResolveNodes()
		if err != nil {
			log.Printf("Follower %s DNS resolution failed: %v", m.config.NodeID, err)
		} else {
			log.Printf("Follower %s initial nodes: %v", m.config.NodeID, nodes)
			err := m.gossip.Join(nodes)
			if err != nil {
				log.Printf("Follower %s failed to join gossip: %v", m.config.NodeID, err)
			}
		}
	}

	// Start periodic gap checking if leader
	if m.isLeader {
		go m.startGapChecking()
	}
	return nil
}

// IsLeader returns if this node is the leader
func (m *Manager) IsLeader() bool {
	return m.isLeader
}

// Members returns the list of cluster members
func (m *Manager) Members() []*memberlist.Node {
	return m.gossip.Members()
}

// tryBecomeLeaderPeriodically tries to become leader periodically
func (m *Manager) tryBecomeLeaderPeriodically() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	log.Printf("Follower %s starting periodic leader election attempts", m.config.NodeID)
	for range ticker.C {
		if m.leaderElection.TryBecomeLeader() {
			m.isLeader = true
			log.Printf("Follower %s promoted to leader", m.config.NodeID)
			// Start DNS watching
			go m.dnsResolver.WatchNodes(m.config.WatchInterval, func(nodes []string) {
				log.Printf("New leader %s discovered nodes: %v", m.config.NodeID, nodes)
				m.gossip.Join(nodes)
			})
			return
		}
	}
}

// startGapChecking starts periodic gap checking
func (m *Manager) startGapChecking() {
	ticker := time.NewTicker(10 * time.Second) // Check every 10 seconds
	defer ticker.Stop()
	log.Printf("Leader %s starting gap checking", m.config.NodeID)
	for range ticker.C {
		if !m.isLeader {
			return
		}
		m.checkFollowerGaps()
	}
}

// Replicate sends the data to all followers
func (m *Manager) Replicate(data interface{}, dataType entity.DataType, partitionKey string) error {
	if !m.isLeader {
		return nil // Only leader replicates
	}

	followers := m.getFollowers()
	log.Printf("Leader %s replicating data to %d followers", m.config.NodeID, len(followers))

	for _, node := range followers {
		go func(n *memberlist.Node) {
			err := m.sendDataToFollower(n, data, dataType, partitionKey)
			if err != nil {
				log.Printf("Failed to replicate to %s: %v", n.Name, err)
			}
		}(node)
	}
	return nil
}

// getFollowers returns the list of follower nodes
func (m *Manager) getFollowers() []*memberlist.Node {
	var followers []*memberlist.Node
	for _, node := range m.gossip.Members() {
		if node.Name != m.config.NodeID {
			followers = append(followers, node)
		}
	}
	return followers
}

// sendDataToFollower sends data to a follower via HTTP
func (m *Manager) sendDataToFollower(node *memberlist.Node, data interface{}, dataType entity.DataType, partitionKey string) error {
	url := fmt.Sprintf("http://%s/replicate", node.Addr.String())
	payload := map[string]interface{}{
		"data":          data,
		"data_type":     dataType,
		"partition_key": partitionKey,
	}
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf("replication failed: %s", string(body))
	}

	log.Printf("Successfully replicated data to %s", node.Name)
	return nil
}

// checkFollowerGaps checks gaps with followers and stores them
func (m *Manager) checkFollowerGaps() {
	if !m.isLeader {
		return
	}

	followers := m.getFollowers()
	log.Printf("Leader %s checking gaps with %d followers", m.config.NodeID, len(followers))

	for _, node := range followers {
		gap, err := m.getGapWithFollower(node)
		if err != nil {
			log.Printf("Failed to get gap with %s: %v", node.Name, err)
			continue
		}
		if gap > 0 {
			m.storeGap(node.Name, gap)
			log.Printf("Stored gap of %d for node %s", gap, node.Name)
		}
	}
}

// getGapWithFollower gets the gap with a follower
func (m *Manager) getGapWithFollower(node *memberlist.Node) (int, error) {
	url := fmt.Sprintf("http://%s/status", node.Addr.String())
	resp, err := http.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	var status map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return 0, err
	}

	// For now, assume leader has offset 10, follower has less
	leaderOffset := 10 // placeholder
	followerOffset := 5 // placeholder from status
	gap := leaderOffset - followerOffset
	return gap, nil
}

// storeGap stores the gap information
func (m *Manager) storeGap(nodeName string, gap int) {
	gapData := map[string]interface{}{
		"node": nodeName,
		"gap":  gap,
		"time": time.Now().Unix(),
	}
	// Store as a record
	if m.usecase != nil {
		err := m.usecase.StoreRecord(gapData, entity.DataTypeJSON, "gaps")
		if err != nil {
			log.Printf("Failed to store gap: %v", err)
		}
	}
}

// GetGaps returns the stored gaps
func (m *Manager) GetGaps() ([]map[string]interface{}, error) {
	// For simplicity, return a placeholder; in real impl, query the storage
	return []map[string]interface{}{
		{"node": "follower1", "gap": 5},
	}, nil
}

// Shutdown shuts down the cluster manager
func (m *Manager) Shutdown() {
	log.Printf("Shutting down cluster manager for node %s", m.config.NodeID)
	if m.isLeader {
		m.leaderElection.Resign()
	}
	m.gossip.Shutdown()
	m.leaderElection.Close()
}