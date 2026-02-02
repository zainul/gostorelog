package cluster

import (
	"context"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

// LeaderElection manages leader election using Redis
type LeaderElection struct {
	client *redis.Client
	key    string
	id     string
	ttl    time.Duration
	isLeader bool
}

// NewLeaderElection creates a new leader election instance
func NewLeaderElection(redisAddr, id string) *LeaderElection {
	client := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})
	return &LeaderElection{
		client:   client,
		key:      "gostorelog:leader",
		id:       id,
		ttl:      10 * time.Second,
		isLeader: false,
	}
}

// TryBecomeLeader attempts to become the leader
func (le *LeaderElection) TryBecomeLeader() bool {
	ctx := context.Background()
	success, err := le.client.SetNX(ctx, le.key, le.id, le.ttl).Result()
	if err != nil {
		log.Printf("Failed to set leader key: %v", err)
		return false
	}
	if success {
		le.isLeader = true
		log.Printf("Node %s became leader", le.id)
		// Start renewal goroutine
		go le.renewLeadership()
		return true
	}
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
	for range ticker.C {
		if !le.isLeader {
			return
		}
		ctx := context.Background()
		err := le.client.Expire(ctx, le.key, le.ttl).Err()
		if err != nil {
			log.Printf("Failed to renew leadership: %v", err)
			le.isLeader = false
			return
		}
	}
}

// Resign resigns from leadership
func (le *LeaderElection) Resign() {
	le.isLeader = false
	ctx := context.Background()
	le.client.Del(ctx, le.key)
}

// Close closes the Redis client
func (le *LeaderElection) Close() error {
	return le.client.Close()
}