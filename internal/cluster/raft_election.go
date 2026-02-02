package cluster

import (
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

// RaftLeaderElection manages leader election using Raft consensus
type RaftLeaderElection struct {
	raft          *raft.Raft
	leaderCh      <-chan bool
	isLeader      bool
	config        *Config
	shutdownCh    chan struct{}
}

// NewRaftLeaderElection creates a new Raft-based leader election
func NewRaftLeaderElection(config *Config, peers []string) (*RaftLeaderElection, error) {
	// Create Raft configuration
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(config.NodeID)
	raftConfig.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   "raft",
		Output: os.Stderr,
		Level:  hclog.Info,
	})

	// Create transport
	addr, err := net.ResolveTCPAddr("tcp", config.BindAddr)
	if err != nil {
		return nil, err
	}
	transport, err := raft.NewTCPTransport(config.BindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Create snapshot store
	snapshots, err := raft.NewFileSnapshotStore(config.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, err
	}

	// Create log store and stable store
	logStore, err := raftboltdb.NewBoltStore(filepath.Join(config.DataDir, "raft-log.db"))
	if err != nil {
		return nil, err
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(config.DataDir, "raft-stable.db"))
	if err != nil {
		return nil, err
	}

	// Create Raft instance
	ra, err := raft.NewRaft(raftConfig, &fsm{}, logStore, stableStore, snapshots, transport)
	if err != nil {
		return nil, err
	}

	// Bootstrap cluster if no peers
	if len(peers) == 0 {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raftConfig.LocalID,
					Address: transport.LocalAddr(),
				},
			},
		}
		ra.BootstrapCluster(configuration)
	} else {
		// Join existing cluster
		for _, peer := range peers {
			// In a real setup, you'd send join requests
			log.Printf("Would join peer: %s", peer)
		}
	}

	rle := &RaftLeaderElection{
		raft:       ra,
		leaderCh:   ra.LeaderCh(),
		config:     config,
		shutdownCh: make(chan struct{}),
	}

	go rle.monitorLeadership()

	return rle, nil
}

// monitorLeadership monitors leadership changes
func (rle *RaftLeaderElection) monitorLeadership() {
	for {
		select {
		case isLeader := <-rle.leaderCh:
			rle.isLeader = isLeader
			if isLeader {
				log.Printf("Node %s elected as Raft leader", rle.config.NodeID)
			} else {
				log.Printf("Node %s is no longer Raft leader", rle.config.NodeID)
			}
		case <-rle.shutdownCh:
			return
		}
	}
}

// IsLeader returns if this node is the leader
func (rle *RaftLeaderElection) IsLeader() bool {
	return rle.isLeader
}

// Shutdown shuts down the Raft election
func (rle *RaftLeaderElection) Shutdown() {
	close(rle.shutdownCh)
	rle.raft.Shutdown()
}

// fsm implements raft.FSM
type fsm struct{}

func (f *fsm) Apply(l *raft.Log) interface{} {
	log.Printf("Raft FSM Apply: %s", string(l.Data))
	return nil
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{}, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	return nil
}

// fsmSnapshot implements raft.FSMSnapshot
type fsmSnapshot struct{}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	return sink.Close()
}

func (f *fsmSnapshot) Release() {}