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
func NewDNSResolver(serviceName, port string) *DNSResolver {
	return &DNSResolver{
		serviceName: serviceName,
		port:        port,
	}
}

// ResolveNodes resolves the list of node addresses
func (dr *DNSResolver) ResolveNodes() ([]string, error) {
	ips, err := net.LookupIP(dr.serviceName)
	if err != nil {
		return nil, err
	}
	var addresses []string
	for _, ip := range ips {
		addresses = append(addresses, net.JoinHostPort(ip.String(), dr.port))
	}
	return addresses, nil
}

// WatchNodes periodically resolves nodes and calls the callback
func (dr *DNSResolver) WatchNodes(interval time.Duration, callback func([]string)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		nodes, err := dr.ResolveNodes()
		if err != nil {
			log.Printf("DNS resolution failed: %v", err)
			continue
		}
		callback(nodes)
	}
}