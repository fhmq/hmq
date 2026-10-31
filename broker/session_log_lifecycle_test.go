package broker

import (
	stdjson "encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/fhmq/hmq/v4/broker/sessionstore"
)

func TestExperimentalSessionLogConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session.log")
	if err := os.WriteFile(path, nil, 0600); err != nil {
		t.Fatal(err)
	}
	alias := filepath.Join(dir, "hardlink")
	if err := os.Link(path, alias); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(dir, "symlink")
	if err := os.Symlink(path, symlink); err != nil && runtime.GOOS != "windows" {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		cfg  Config
	}{
		{"blank", Config{ExperimentalSessionLogPath: "  "}},
		{"whitespace", Config{ExperimentalSessionLogPath: " " + path}},
		{"retained", Config{ExperimentalSessionLogPath: path, RetainedLogPath: filepath.Join(dir, ".", "session.log")}},
		{"hardlink", Config{ExperimentalSessionLogPath: path, RetainedLogPath: alias}},
		{"event", Config{ExperimentalSessionLogPath: path, Cluster: RouteInfo{Reliable: ReliableConfig{EventLogPath: path}}}},
		{"inbox", Config{ExperimentalSessionLogPath: path, Cluster: RouteInfo{Reliable: ReliableConfig{InboxLogPath: path}}}},
	}
	if runtime.GOOS != "windows" {
		tests = append(tests, struct {
			name string
			cfg  Config
		}{"symlink", Config{ExperimentalSessionLogPath: symlink, RetainedLogPath: path}})
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateSessionLogPath(&tt.cfg); err == nil {
				t.Fatal("session path validation accepted invalid path")
			}
			if err := tt.cfg.check(); err == nil {
				t.Fatal("config check accepted invalid path")
			}
			if b, err := NewBroker(&tt.cfg); err == nil {
				b.Close()
				t.Fatal("NewBroker accepted invalid path")
			}
		})
	}
	configPath := filepath.Join(dir, "config.json")
	data, err := stdjson.Marshal(map[string]string{"experimentalSessionLogPath": path})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configPath, data, 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(configPath)
	if err != nil || cfg.ExperimentalSessionLogPath != path {
		t.Fatalf("config = %+v, err = %v", cfg, err)
	}
	if err := cfg.check(); err != nil {
		t.Fatal(err)
	}
}

func TestExperimentalSessionLogLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "session.log")
	s, err := sessionstore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Put("saved", 0, &sessionstore.State{Subscriptions: map[string]byte{"a": 1}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := NewBroker(&Config{ExperimentalSessionLogPath: path})
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := b.sessionLog.Get("saved")
	if err != nil || state == nil || state.Subscriptions["a"] != 1 {
		t.Fatalf("replay = %+v, %v", state, err)
	}
	if _, err := b.sessionMgr.Get("saved"); err == nil {
		t.Fatal("experimental log unexpectedly enabled session recovery")
	}
	if other, err := NewBroker(&Config{ExperimentalSessionLogPath: path}); err == nil {
		other.Close()
		t.Fatal("second broker acquired log")
	}
	b.Close()
	b.Close()
	b, err = NewBroker(&Config{ExperimentalSessionLogPath: path})
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
}

func TestExperimentalSessionLogStartupFailureCleanup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session.log")
	retained := filepath.Join(dir, "retained.log")
	if err := os.WriteFile(path, []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	if b, err := NewBroker(&Config{ExperimentalSessionLogPath: path, RetainedLogPath: retained}); err == nil {
		b.Close()
		t.Fatal("corrupt log accepted")
	}
	// Opening the retained store again proves the earlier initialization was cleaned up.
	b, err := NewBroker(&Config{RetainedLogPath: retained})
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if b, err := NewBroker(&Config{ExperimentalSessionLogPath: path, TlsPort: "8883"}); err == nil {
		b.Close()
		t.Fatal("invalid TLS accepted")
	}
	s, err := sessionstore.Open(path)
	if err != nil {
		t.Fatalf("session log lock leaked after TLS failure: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}
