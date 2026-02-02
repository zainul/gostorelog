package cluster

import (
	"context"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

// LeaderElection manages leader election using Redis or Raft fallback
type LeaderElection struct {
	redisClient   *redis.Client
	raftElection  *RaftLeaderElection
	key           string
	id            string
	ttl           time.Duration
	isLeader      bool
	useRaft       bool
	config        *Config
}

// NewLeaderElection creates a new leader election instance
func NewLeaderElection(config *Config, peers []string) (*LeaderElection, error) {
	client := redis.NewClient(&redis.Options{
		Addr: config.RedisAddr,
	})

	le := &LeaderElection{
		redisClient: client,
		key:         config.LeaderKey,
		id:          config.NodeID,
		ttl:         config.LeaderTTL,
		config:      config,
	}

	// Try Redis first
	if le.tryRedis() {
		log.Printf("Using Redis for leader election")
		return le, nil
	}

	// Fallback to Raft
	log.Printf("Redis unavailable, falling back to Raft consensus")
	raftElection, err := NewRaftLeaderElection(config, peers)
	if err != nil {
		return nil, err
	}
	le.raftElection = raftElection
	le.useRaft = true
	log.Printf("Raft leader election initialized for node %s", config.NodeID)
	return le, nil
}

// tryRedis attempts to connect to Redis
func (le *LeaderElection) tryRedis() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := le.redisClient.Ping(ctx).Result()
	return err == nil
}

// TryBecomeLeader attempts to become the leader
func (le *LeaderElection) TryBecomeLeader() bool {
	if le.useRaft {
		// Raft handles leadership internally
		return le.raftElection.IsLeader()
	}

	ctx := context.Background()
	log.Printf("Attempting to become leader for node %s using Redis", le.id)
	success, err := le.redisClient.SetNX(ctx, le.key, le.id, le.ttl).Result()
	if err != nil {
		log.Printf("Leader election failed for node %s: %v", le.id, err)
		return false
	}
	if success {
		le.isLeader = true
		log.Printf("SUCCESS: Node %s promoted to leader via Redis", le.id)
		// Start renewal goroutine
		go le.renewLeadership()
		return true
	}
	log.Printf("Leader election failed: key already exists for node %s", le.id)
	return false
}

// IsLeader checks if this node is the leader
func (le *LeaderElection) IsLeader() bool {
	if le.useRaft {
		return le.raftElection.IsLeader()
	}
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
		err := le.redisClient.Expire(ctx, le.key, le.ttl).Err()
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
	if le.useRaft {
		// Raft handles resignation internally
		return
	}
	ctx := context.Background()
	le.redisClient.Del(ctx, le.key)
	log.Printf("Node %s resigned from leadership", le.id)
}

// Close closes the clients
func (le *LeaderElection) Close() error {
	if le.useRaft {
		le.raftElection.Shutdown()
		return nil
	}
	return le.redisClient.Close()
}