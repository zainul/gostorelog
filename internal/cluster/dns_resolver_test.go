package cluster

import (
	"testing"
)

func TestDNSResolver_ResolveNodes(t *testing.T) {
	config := DefaultConfig()
	config.ServiceName = "localhost" // Use localhost for test
	config.ClusterPort = "7946"

	dr := NewDNSResolver(config)

	t.Logf("Scenario: Resolving nodes for service %s on port %s", config.ServiceName, config.ClusterPort)
	t.Logf("Input: ServiceName=%s, ClusterPort=%s", config.ServiceName, config.ClusterPort)

	addresses, err := dr.ResolveNodes()
	t.Logf("Output: ResolveNodes returned addresses=%v, error=%v", addresses, err)

	if err != nil {
		t.Logf("Result: DNS resolution failed as expected (no real service), error: %v", err)
	} else {
		t.Logf("Result: Resolved %d addresses", len(addresses))
	}
}