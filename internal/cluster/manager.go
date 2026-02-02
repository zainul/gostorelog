package cluster

import (
	"log"
	"time"

	"github.com/hashicorp/memberlist"
)

// Manager manages the cluster
type Manager struct {
	leaderElection *LeaderElection
	gossip         *Gossip
	dnsResolver    *DNSResolver
	nodeID         string
	isLeader       bool
}

// NewManager creates a new cluster manager
func NewManager(nodeID, bindAddr, advertiseAddr, redisAddr, serviceName, port string) (*Manager, error) {
	le := NewLeaderElection(redisAddr, nodeID)
	gossip, err := NewGossip(bindAddr, advertiseAddr)
	if err != nil {
		return nil, err
	}
	dns := NewDNSResolver(serviceName, port)

	return &Manager{
		leaderElection: le,
		gossip:         gossip,
		dnsResolver:    dns,
		nodeID:         nodeID,
	}, nil
}

// Start starts the cluster manager
func (m *Manager) Start() error {
	// Try to become leader
	if m.leaderElection.TryBecomeLeader() {
		m.isLeader = true
		log.Printf("This node is the leader")
		// As leader, start DNS watching to discover nodes
		go m.dnsResolver.WatchNodes(30*time.Second, func(nodes []string) {
			log.Printf("Discovered nodes: %v", nodes)
			// Join the gossip cluster
			err := m.gossip.Join(nodes)
			if err != nil {
				log.Printf("Failed to join gossip: %v", err)
			}
		})
	} else {
		log.Printf("This node is a follower")
		// As follower, periodically try to become leader
		go m.tryBecomeLeaderPeriodically()
		// Discover initial nodes via DNS
		nodes, err := m.dnsResolver.ResolveNodes()
		if err != nil {
			log.Printf("DNS resolution failed: %v", err)
		} else {
			err := m.gossip.Join(nodes)
			if err != nil {
				log.Printf("Failed to join gossip: %v", err)
			}
		}
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
	for range ticker.C {
		if m.leaderElection.TryBecomeLeader() {
			m.isLeader = true
			log.Printf("Promoted to leader")
			// Start DNS watching
			go m.dnsResolver.WatchNodes(30*time.Second, func(nodes []string) {
				log.Printf("Discovered nodes: %v", nodes)
				m.gossip.Join(nodes)
			})
			return
		}
	}
}

// Shutdown shuts down the cluster manager
func (m *Manager) Shutdown() {
	m.leaderElection.Resign()
	m.gossip.Shutdown()
	m.leaderElection.Close()
}