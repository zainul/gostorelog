package cluster

import (
	"log"
	"net"
	"time"
)

// DNSResolver resolves node addresses via DNS
type DNSResolver struct {
	serviceName string
	port        string
}

// NewDNSResolver creates a new DNS resolver
func NewDNSResolver(config *Config) *DNSResolver {
	log.Printf("Initializing DNS resolver for service %s on port %s", config.ServiceName, config.ClusterPort)
	return &DNSResolver{
		serviceName: config.ServiceName,
		port:        config.ClusterPort,
	}
}

// ResolveNodes resolves the list of node addresses
func (dr *DNSResolver) ResolveNodes() ([]string, error) {
	log.Printf("Resolving DNS for service %s", dr.serviceName)
	ips, err := net.LookupIP(dr.serviceName)
	if err != nil {
		log.Printf("DNS lookup failed for %s: %v", dr.serviceName, err)
		return nil, err
	}
	var addresses []string
	for _, ip := range ips {
		addr := net.JoinHostPort(ip.String(), dr.port)
		addresses = append(addresses, addr)
	}
	log.Printf("Resolved addresses: %v", addresses)
	return addresses, nil
}

// WatchNodes periodically resolves nodes and calls the callback
func (dr *DNSResolver) WatchNodes(interval time.Duration, callback func([]string)) {
	log.Printf("Starting DNS watch with interval %v", interval)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		nodes, err := dr.ResolveNodes()
		if err != nil {
			log.Printf("DNS watch resolution failed: %v", err)
			continue
		}
		log.Printf("DNS watch discovered nodes: %v", nodes)
		callback(nodes)
	}
}