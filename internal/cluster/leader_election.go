package cluster

import (
	"context"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

// LeaderElection manages leader election using Redis
type LeaderElection struct {
	client   *redis.Client
	key      string
	id       string
	ttl      time.Duration
	isLeader bool
}

// NewLeaderElection creates a new leader election instance
func NewLeaderElection(config *Config) *LeaderElection {
	client := redis.NewClient(&redis.Options{
		Addr: config.RedisAddr,
	})
	log.Printf("Initializing leader election for node %s with Redis at %s, key %s, TTL %v", config.NodeID, config.RedisAddr, config.LeaderKey, config.LeaderTTL)
	return &LeaderElection{
		client:   client,
		key:      config.LeaderKey,
		id:       config.NodeID,
		ttl:      config.LeaderTTL,
		isLeader: false,
	}
}

// TryBecomeLeader attempts to become the leader
func (le *LeaderElection) TryBecomeLeader() bool {
	ctx := context.Background()
	log.Printf("Attempting to become leader for node %s", le.id)
	success, err := le.client.SetNX(ctx, le.key, le.id, le.ttl).Result()
	if err != nil {
		log.Printf("Leader election failed for node %s: %v", le.id, err)
		return false
	}
	if success {
		le.isLeader = true
		log.Printf("SUCCESS: Node %s promoted to leader", le.id)
		// Start renewal goroutine
		go le.renewLeadership()
		return true
	}
	log.Printf("Leader election failed: key already exists for node %s", le.id)
	return false
}

// IsLeader checks if this node is the leader
func (le *LeaderElection) IsLeader() bool {
	return le.isLeader
}

// renewLeadership renews the leadership periodically
func (le *LeaderElection) renewLeadership() {
	ticker := time.NewTicker(le.ttl / 2)
	defer ticker.Stop()
	log.Printf("Starting leadership renewal for node %s", le.id)
	for range ticker.C {
		if !le.isLeader {
			log.Printf("Stopped leadership renewal for node %s", le.id)
			return
		}
		ctx := context.Background()
		err := le.client.Expire(ctx, le.key, le.ttl).Err()
		if err != nil {
			log.Printf("Leadership renewal failed for node %s: %v", le.id, err)
			le.isLeader = false
			return
		}
		log.Printf("Leadership renewed for node %s", le.id)
	}
}

// Resign resigns from leadership
func (le *LeaderElection) Resign() {
	le.isLeader = false
	ctx := context.Background()
	le.client.Del(ctx, le.key)
	log.Printf("Node %s resigned from leadership", le.id)
}

// Close closes the Redis client
func (le *LeaderElection) Close() error {
	return le.client.Close()
}