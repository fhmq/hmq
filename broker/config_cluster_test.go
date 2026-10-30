package broker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDirectClusterConfig(t *testing.T) {
	valid := RouteInfo{NodeID: "n", ClusterID: "c", Secret: "private-key", Listen: "127.0.0.1:1993", Seeds: []string{"[::1]:1994"}, TLS: ClusterTLSInfo{CertFile: "../ssl/server/cert.pem", KeyFile: "../ssl/server/key.pem", CAFile: "../ssl/ca/ca.pem"}}
	for _, tc := range []struct {
		name    string
		cluster RouteInfo
		bad     bool
	}{
		{"disabled", RouteInfo{}, false}, {"valid", valid, false},
		{"missing", RouteInfo{NodeID: "n"}, true},
		{"invalid node id", RouteInfo{NodeID: "bad id", ClusterID: "c", Secret: "private-key", Listen: ":1993", TLS: valid.TLS}, true},
		{"bad listen", RouteInfo{NodeID: "n", ClusterID: "c", Secret: "private-key", Listen: "bad"}, true},
		{"bad seed", RouteInfo{NodeID: "n", ClusterID: "c", Secret: "private-key", Listen: ":1993", Seeds: []string{":1994"}}, true},
		{"legacy", RouteInfo{Port: "1993"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := (&Config{Cluster: tc.cluster}).check()
			if (err != nil) != tc.bad {
				t.Fatalf("unexpected error: %v", err)
			}
			if err != nil && strings.Contains(err.Error(), "private-key") {
				t.Fatal("secret leaked")
			}
		})
	}
}

func TestNewBrokerRejectsLegacyRouter(t *testing.T) {
	for _, cfg := range []Config{{Router: "127.0.0.1:1234"}, {Cluster: RouteInfo{Host: "127.0.0.1"}}, {Cluster: RouteInfo{Port: "1234"}}} {
		b, err := NewBroker(&cfg)
		if err == nil {
			b.Close()
			t.Fatal("legacy router configuration accepted")
		}
	}
}

func TestClusterTLSValidation(t *testing.T) {
	valid := RouteInfo{NodeID: "n", ClusterID: "c", Secret: "private-key", Listen: ":1993", TLS: ClusterTLSInfo{CertFile: "../ssl/server/cert.pem", KeyFile: "../ssl/server/key.pem", CAFile: "../ssl/ca/ca.pem"}}
	badCA := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(badCA, []byte("not a certificate"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*RouteInfo){
		func(c *RouteInfo) { c.TLS.CertFile = "" },
		func(c *RouteInfo) { c.TLS.KeyFile = "" },
		func(c *RouteInfo) { c.TLS.CAFile = "" },
		func(c *RouteInfo) { c.TLS.KeyFile = "../ssl/client/key.pem" },
		func(c *RouteInfo) { c.TLS.CAFile = badCA },
		func(c *RouteInfo) { c.TLS.CAFile = "private-key-missing" },
	} {
		c := valid
		change(&c)
		err := (&Config{Cluster: c}).check()
		if err == nil || strings.Contains(err.Error(), "private-key") {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if err := (&Config{Cluster: RouteInfo{TLS: valid.TLS}}).check(); err == nil {
		t.Fatal("TLS alone must not enable an incomplete cluster")
	}
}

func TestClusterConfigLoading(t *testing.T) {
	for _, input := range []string{`{"router":""}`, `{"Router":""}`, `{"CLUSTER":{"HOST":""}}`, `{"cluster":{"PORT":""}}`, `{"cluster":{"host":""}}`, `{"cluster":{"port":""}}`, `{"cluster":{"port":"1993"}}`, `{"cluster":{"secret":"private-key","seeds":123}}`} {
		path := filepath.Join(t.TempDir(), "config.json")
		if err := os.WriteFile(path, []byte(input), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := LoadConfig(path)
		if err == nil || strings.Contains(err.Error(), "private-key") {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	for _, flag := range []string{"-router=", "-r=", "-cp=", "-clusterport="} {
		if _, err := ConfigureConfig([]string{flag}); err == nil {
			t.Fatalf("expected %s rejection", flag)
		}
	}
}

func TestReliableConfigValidation(t *testing.T) {
	dir := t.TempDir()
	base := RouteInfo{NodeID: "n", ClusterID: "c", Secret: "secret", Listen: ":1993", TLS: ClusterTLSInfo{CertFile: "../ssl/server/cert.pem", KeyFile: "../ssl/server/key.pem", CAFile: "../ssl/ca/ca.pem"}, Reliable: ReliableConfig{Enabled: true, MemberIDs: []string{"n", "peer"}, EventLogPath: filepath.Join(dir, "events"), InboxLogPath: filepath.Join(dir, "inbox")}}
	alias := filepath.Join(dir, "alias")
	if err := os.Symlink(dir, alias); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		change func(*Config)
		bad    bool
	}{
		{"valid", func(*Config) {}, false},
		{"disabled settings", func(c *Config) { c.Cluster.Reliable.Enabled = false }, true},
		{"no cluster", func(c *Config) { c.Cluster.Listen = "" }, true},
		{"missing self", func(c *Config) { c.Cluster.Reliable.MemberIDs = []string{"a", "b"} }, true},
		{"no peer", func(c *Config) { c.Cluster.Reliable.MemberIDs = []string{"n"} }, true},
		{"duplicate", func(c *Config) { c.Cluster.Reliable.MemberIDs = []string{"n", "n"} }, true},
		{"invalid member", func(c *Config) { c.Cluster.Reliable.MemberIDs = []string{"n", "bad id"} }, true},
		{"blank path", func(c *Config) { c.Cluster.Reliable.InboxLogPath = " " }, true},
		{"same path", func(c *Config) { c.Cluster.Reliable.InboxLogPath = filepath.Join(alias, "events") }, true},
		{"retained collision", func(c *Config) { c.RetainedLogPath = c.Cluster.Reliable.EventLogPath }, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := Config{Cluster: base}
			tc.change(&c)
			if err := c.check(); (err != nil) != tc.bad {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestReliableConfigEntry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	input := `{"cluster":{"nodeID":"n","clusterID":"c","secret":"secret","listen":":1993","tls":{"certFile":"../ssl/server/cert.pem","keyFile":"../ssl/server/key.pem","caFile":"../ssl/ca/ca.pem"},"reliable":{"enabled":true,"memberIDs":["n","peer"],"eventLogPath":"events.log","inboxLogPath":"inbox.log"}}}`
	if err := os.WriteFile(path, []byte(input), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := LoadConfig(path)
	if err != nil || !c.Cluster.Reliable.Enabled || len(c.Cluster.Reliable.MemberIDs) != 2 {
		t.Fatalf("load: %#v, %v", c, err)
	}
	if _, err := ConfigureConfig([]string{"-config", path}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
