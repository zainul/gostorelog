package cluster

import "time"

// Config holds cluster configuration
type Config struct {
	NodeID         string        `json:"node_id"`
	BindAddr       string        `json:"bind_addr"`
	AdvertiseAddr  string        `json:"advertise_addr"`
	RedisAddr      string        `json:"redis_addr"`
	ServiceName    string        `json:"service_name"`
	ClusterPort    string        `json:"cluster_port"`
	DataDir        string        `json:"data_dir"`
	WatchInterval  time.Duration `json:"watch_interval"`
	LeaderKey      string        `json:"leader_key"`
	LeaderTTL      time.Duration `json:"leader_ttl"`
}

// DefaultConfig returns a default cluster configuration
func DefaultConfig() *Config {
	return &Config{
		NodeID:         "node1",
		BindAddr:       "0.0.0.0:7946",
		AdvertiseAddr:  "127.0.0.1:7946",
		RedisAddr:      "localhost:6379",
		ServiceName:    "gostorelog-cluster",
		ClusterPort:    "7946",
		DataDir:        "./data",
		WatchInterval:  30 * time.Second,
		LeaderKey:      "gostorelog:leader",
		LeaderTTL:      10 * time.Second,
	}
}