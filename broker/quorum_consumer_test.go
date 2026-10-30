package broker

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/hashicorp/raft"
)

func TestQuorumConsumerRetryAndWALRecovery(t *testing.T) {
	c := quorumTestConfigs(t)[0].Cluster
	c.Quorum.Peers = map[string]string{c.NodeID: c.Quorum.Listen}
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
	var attempts atomic.Int32
	var fail atomic.Bool
	fail.Store(true)
	deliver := func(*packets.PublishPacket) error {
		attempts.Add(1)
		if fail.Load() {
			return errors.New("retry delivery")
		}
		return nil
	}
	open := func() *quorumRuntime {
		r, err := openQuorumRuntime(c, &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}, &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: roots})
		if err != nil {
			t.Fatal(err)
		}
		r.startConsumer(c.NodeID, c.ClusterID, deliver)
		t.Cleanup(func() { _ = r.Close() })
		for deadline := time.Now().Add(8 * time.Second); time.Now().Before(deadline); {
			if r.node.State() == raft.Leader {
				return r
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("single voter did not elect leader")
		return nil
	}
	wait := func(condition func() bool) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
			if condition() {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("consumer condition timed out")
	}
	r := open()
	if err := r.publish(&packets.PublishPacket{TopicName: "test", Payload: []byte("one")}); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { return attempts.Load() >= 2 && r.health().ConsumerFailed })
	if r.health().Cursor != 0 {
		t.Fatal("cursor advanced on failed delivery")
	}
	fail.Store(false)
	wait(func() bool { return r.health().Cursor > 0 && !r.health().ConsumerFailed })
	first := r.health().Cursor
	_ = r.Close()
	r = open()
	if r.health().Cursor != first {
		t.Fatal("cursor lost on reopen")
	}
	count := attempts.Load()
	time.Sleep(100 * time.Millisecond)
	if attempts.Load() != count {
		t.Fatal("already checkpointed entry replayed")
	}
	// A successful delivery followed by a failed WAL append cannot advance
	// the cursor. Reopening must replay that entry, allowing duplicates.
	if err := r.cursorLog.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.publish(&packets.PublishPacket{TopicName: "test", Payload: []byte("two")}); err != nil {
		t.Fatal(err)
	}
	wait(func() bool { return r.health().ConsumerFailed })
	if r.health().Cursor != first {
		t.Fatal("cursor advanced despite failed WAL append")
	}
	count = attempts.Load()
	_ = r.Close()
	r = open()
	wait(func() bool { return r.health().Cursor > first })
	if attempts.Load() <= count {
		t.Fatal("uncheckpointed delivery was not replayed")
	}
}

func TestQuorumCursorIdentity(t *testing.T) {
	dir := t.TempDir()
	l, _, err := openQuorumCursor(dir, "a", "cluster")
	if err != nil {
		t.Fatal(err)
	}
	_ = l.Close()
	for _, identity := range [][2]string{{"b", "cluster"}, {"a", "other"}} {
		if l, _, err := openQuorumCursor(dir, identity[0], identity[1]); err == nil {
			_ = l.Close()
			t.Fatal("accepted wrong cursor identity")
		}
	}
}

func TestQuorumCloseBeforeConsumerStart(t *testing.T) {
	c := quorumTestConfigs(t)[0].Cluster
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
	r, err := openQuorumRuntime(c, &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert}, &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: roots})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- r.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close waited for unstarted consumer")
	}
	r.startConsumer(c.NodeID, c.ClusterID, func(*packets.PublishPacket) error { t.Error("consumer started after close"); return nil })
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
}
