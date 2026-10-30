package broker

import (
	"context"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestUnixListenerDoesNotRemoveExistingFile(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Unix sockets are not used on Windows")
	}
	path := filepath.Join(t.TempDir(), "socket")
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	b, err := NewBroker(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); b.StartUnixSocketClientListening(path, true) }()
	time.Sleep(50 * time.Millisecond)
	b.Close()
	<-done
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "keep" {
		t.Fatalf("existing file changed: %q, %v", data, err)
	}
}

func TestCloseStopsListenerAndPendingHandshake(t *testing.T) {
	b, err := NewBroker(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if !b.registerListener(l) {
		t.Fatal("listener rejected")
	}
	server, peer := net.Pipe()
	defer peer.Close()
	done := make(chan struct{})
	go func() { defer close(done); _ = b.handleConnection(CLIENT, server) }()
	deadline := time.Now().Add(time.Second)
	for {
		b.lifecycleMu.Lock()
		n := len(b.connections)
		b.lifecycleMu.Unlock()
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("connection was not registered")
		}
		time.Sleep(time.Millisecond)
	}
	b.Close()
	b.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handshake did not stop")
	}
	if _, err := l.Accept(); err == nil {
		t.Fatal("listener still open")
	}
}

func TestCloseWaitsForActiveHTTPHandler(t *testing.T) {
	b, err := NewBroker(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	h := b.trackHTTP(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { close(started); <-release }))
	go h.ServeHTTP(nil, nil)
	<-started
	go func() { b.Close(); close(done) }()
	select {
	case <-done:
		t.Fatal("close returned while handler was active")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("close blocked")
	}
}

func TestCloseCleansRegisteredClient(t *testing.T) {
	b, err := NewBroker(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &client{broker: b, typ: CLIENT, ctx: ctx, cancelFunc: cancel, info: info{clientID: "test"}}
	b.clients.Store("test", c)
	b.Close()
	c.Close()
	if _, ok := b.clients.Load("test"); ok {
		t.Fatal("client remains registered")
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("client not canceled")
	}
}

func TestCloseClearsSessions(t *testing.T) {
	b, err := NewBroker(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.sessionMgr.New("test-session"); err != nil {
		t.Fatal(err)
	}
	b.Close()
	if got := b.sessionMgr.Count(); got != 0 {
		t.Fatalf("sessions after close: %d", got)
	}
}

func TestCloseWaitsForJobsAndClosesHTTP(t *testing.T) {
	b, err := NewBroker(&Config{})
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if !b.registerListener(l) {
		t.Fatal("listener rejected")
	}
	s := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})}
	if !b.registerServer(s) {
		t.Fatal("server rejected")
	}
	if !b.launch(func() { _ = s.Serve(l) }) {
		t.Fatal("server job rejected")
	}
	started, release := make(chan struct{}), make(chan struct{})
	b.launch(func() { close(started); <-release })
	<-started
	done := make(chan struct{})
	go func() { b.Close(); close(done) }()
	select {
	case <-done:
		t.Fatal("close did not wait")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("close blocked")
	}
	if _, err := net.DialTimeout("tcp", l.Addr().String(), time.Second); err == nil {
		t.Fatal("HTTP listener still open")
	}
	if b.launch(func() {}) {
		t.Fatal("job accepted after close")
	}
}
