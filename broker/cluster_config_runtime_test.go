package broker

import "testing"

func TestNewBrokerRejectsUnavailableReliableMode(t *testing.T) {
	b, err := NewBroker(&Config{Cluster: RouteInfo{Reliable: ReliableConfig{Enabled: true}}})
	if err == nil {
		b.Close()
		t.Fatal("unavailable reliable mode accepted")
	}
}

func TestBrokerUsesConfigurationSnapshot(t *testing.T) {
	config := &Config{Host: "127.0.0.1", Worker: 1}
	b, err := NewBroker(config)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	config.Host = "changed"
	config.Worker = 7
	if b.config.Host != "127.0.0.1" || b.config.Worker != 1 {
		t.Fatalf("broker configuration changed: %+v", b.config)
	}
}

func TestNewBrokerRejectsPartialClusterConfiguration(t *testing.T) {
	for _, config := range []RouteInfo{
		{NodeID: "node"},
		{ClusterID: "cluster"},
		{Secret: "secret"},
		{Seeds: []string{"127.0.0.1:1234"}},
		{TLS: ClusterTLSInfo{CAFile: "ca.pem"}},
	} {
		b, err := NewBroker(&Config{Cluster: config})
		if err == nil {
			b.Close()
			t.Fatal("partial cluster configuration accepted")
		}
	}
}
