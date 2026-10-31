package broker

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/fhmq/hmq/v4/broker/replication"
	"github.com/hashicorp/raft"
)

type quorumCloseFunc func() error

func (f quorumCloseFunc) Close() error { return f() }

func TestQuorumShutdownInterruptsNodeWait(t *testing.T) {
	interrupted := make(chan struct{})
	var interrupt sync.Once
	unblock := func() { interrupt.Do(func() { close(interrupted) }) }
	defer unblock()
	nodeErr, transportErr := errors.New("node close"), errors.New("transport close")
	done := make(chan error, 1)
	go func() {
		done <- closeQuorumNetwork(
			quorumCloseFunc(func() error { <-interrupted; return nodeErr }),
			quorumCloseFunc(func() error { unblock(); return transportErr }),
		)
	}()
	select {
	case err := <-done:
		if !errors.Is(err, nodeErr) || !errors.Is(err, transportErr) {
			t.Fatal("lost close error:", err)
		}
	case <-time.After(time.Second):
		t.Fatal("node close blocked transport interruption")
	}
}

func TestQuorumCloseInterruptsStalledSnapshot(t *testing.T) {
	configs := quorumTestConfigs(t)
	tlsConfigs := func(c RouteInfo) (*tls.Config, *tls.Config) {
		cert, err := tls.LoadX509KeyPair(c.TLS.CertFile, c.TLS.KeyFile)
		if err != nil {
			t.Fatal(err)
		}
		ca, err := os.ReadFile(c.TLS.CAFile)
		if err != nil {
			t.Fatal(err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(ca) {
			t.Fatal("bad CA")
		}
		return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}, &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, RootCAs: roots}
	}
	a, b := configs[0].Cluster, configs[1].Cluster
	server, client := tlsConfigs(b)
	layer, err := replication.ListenTLS(replication.TLSStreamConfig{ID: b.NodeID, ClusterID: b.ClusterID, Listen: b.Quorum.Listen, Peers: b.Quorum.Peers, ServerTLS: server, ClientTLS: client, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	receiver := raft.NewNetworkTransport(layer, 3, 5*time.Second, io.Discard)
	t.Cleanup(func() { _ = receiver.Close() })
	server, client = tlsConfigs(a)
	r, err := openQuorumRuntime(a, server, client)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = receiver.Close(); _ = r.Close() })
	rpcDone := make(chan error, 1)
	go func() {
		// The peer deliberately neither reads the advertised snapshot nor responds.
		// Its advertised size yields a 320s transport deadline, not our 3s bound.
		rpcDone <- r.transport.InstallSnapshot(raft.ServerID(b.NodeID), raft.ServerAddress(b.Quorum.Listen),
			&raft.InstallSnapshotRequest{RPCHeader: raft.RPCHeader{ProtocolVersion: raft.ProtocolVersionMax}, SnapshotVersion: 1, Term: 1, LastLogIndex: 1, LastLogTerm: 1, Size: 64 * int64(r.transport.TimeoutScale)},
			&raft.InstallSnapshotResponse{}, bytes.NewReader([]byte("incomplete")))
	}()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case rpc := <-receiver.Consumer():
			if _, ok := rpc.Command.(*raft.InstallSnapshotRequest); !ok {
				rpc.Respond(nil, errors.New("peer paused"))
				continue
			}
		case err := <-rpcDone:
			t.Fatalf("snapshot did not stall: %v", err)
		case <-deadline.C:
			t.Fatal("snapshot did not reach authenticated peer")
		}
		break
	}
	closed := make(chan error, 1)
	go func() { closed <- r.Close() }()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-timer.C:
		t.Fatal("Close waited for size-scaled snapshot timeout")
	}
	select {
	case err := <-rpcDone:
		if err == nil {
			t.Fatal("stalled snapshot unexpectedly succeeded")
		}
	case <-timer.C:
		t.Fatal("Close left snapshot RPC blocked")
	}
}
