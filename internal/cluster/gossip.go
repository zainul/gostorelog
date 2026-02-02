package cluster

import (
	"log"

	"github.com/hashicorp/memberlist"
)

// Gossip manages node discovery using gossip protocol
type Gossip struct {
	list   *memberlist.Memberlist
	events *eventDelegate
	nodes  map[string]*memberlist.Node
}

// NewGossip creates a new gossip instance
func NewGossip(config *Config) (*Gossip, error) {
	memberlistConfig := memberlist.DefaultLANConfig()
	memberlistConfig.BindAddr = config.BindAddr
	memberlistConfig.AdvertiseAddr = config.AdvertiseAddr
	events := &eventDelegate{}
	memberlistConfig.Events = events

	log.Printf("Initializing gossip for node %s, bind %s, advertise %s", config.NodeID, config.BindAddr, config.AdvertiseAddr)
	list, err := memberlist.Create(memberlistConfig)
	if err != nil {
		return nil, err
	}

	g := &Gossip{
		list:   list,
		events: events,
		nodes:  make(map[string]*memberlist.Node),
	}

	g.events.gossip = g
	return g, nil
}

// Join joins the cluster with known nodes
func (g *Gossip) Join(knownNodes []string) error {
	log.Printf("Joining gossip cluster with known nodes: %v", knownNodes)
	_, err := g.list.Join(knownNodes)
	if err != nil {
		log.Printf("Failed to join gossip cluster: %v", err)
	} else {
		log.Printf("Successfully joined gossip cluster")
	}
	return err
}

// Members returns the list of known members
func (g *Gossip) Members() []*memberlist.Node {
	return g.list.Members()
}

// Shutdown shuts down the gossip
func (g *Gossip) Shutdown() error {
	log.Printf("Shutting down gossip")
	return g.list.Shutdown()
}

// eventDelegate handles memberlist events
type eventDelegate struct {
	gossip *Gossip
}

func (e *eventDelegate) NotifyJoin(node *memberlist.Node) {
	log.Printf("Node joined: %s", node.Name)
	e.gossip.nodes[node.Name] = node
}

func (e *eventDelegate) NotifyLeave(node *memberlist.Node) {
	log.Printf("Node left: %s", node.Name)
	delete(e.gossip.nodes, node.Name)
}

func (e *eventDelegate) NotifyUpdate(node *memberlist.Node) {
	log.Printf("Node updated: %s", node.Name)
}