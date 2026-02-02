package cluster

import (
	"log"
	"time"

	"github.com/hashicorp/memberlist"
)

// Manager manages the cluster
type Manager struct {
	config *Config
	leaderElection *LeaderElection
	gossip         *Gossip
	dnsResolver    *DNSResolver
	isLeader       bool
}

// NewManager creates a new cluster manager
func NewManager(config *Config) (*Manager, error) {
	le := NewLeaderElection(config)
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

// Shutdown shuts down the cluster manager
func (m *Manager) Shutdown() {
	log.Printf("Shutting down cluster manager for node %s", m.config.NodeID)
	if m.isLeader {
		m.leaderElection.Resign()
	}
	m.gossip.Shutdown()
	m.leaderElection.Close()
}