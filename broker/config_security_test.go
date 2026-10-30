package broker

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{"default worker", Config{}, false},
		{"negative worker", Config{Worker: -1}, true},
		{"tls missing certificate", Config{TlsPort: "8883"}, true},
		{"websocket tls missing certificate", Config{WsPort: "8080", WsTLS: true}, true},
		{"disabled websocket tls", Config{WsTLS: true}, false},
		{"router without cluster", Config{Router: "router"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.config.check()
			if (err != nil) != tc.wantErr {
				t.Fatalf("check() error = %v", err)
			}
		})
	}
}

func TestConfigureConfigInvalidFlagReturnsError(t *testing.T) {
	if _, err := ConfigureConfig([]string{"-unknown"}); err == nil {
		t.Fatal("expected flag error")
	}
}

func TestLoadConfigErrors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.json")
	if _, err := LoadConfig(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected missing file error, got %v", err)
	}
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(path); err == nil {
		t.Fatal("expected invalid JSON error")
	}
}

func TestNewTLSConfigPreservesFileError(t *testing.T) {
	_, err := NewTLSConfig(TLSInfo{CertFile: filepath.Join(t.TempDir(), "missing")})
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected missing file error, got %v", err)
	}
}
