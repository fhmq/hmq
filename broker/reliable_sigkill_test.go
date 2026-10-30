package broker

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/fhmq/hmq/v3/broker/eventstore"
	"github.com/fhmq/hmq/v3/broker/reliable/inbox"
)

func TestReliableSIGKILLAndLostACK(t *testing.T) {
	dir := os.Getenv("HMQ_SIGKILL_DIR")
	if dir != "" {
		wire := runtimeSender{make(chan runtimeMessage, 100)}
		r, err := newBoundReliableRuntime("a", []string{"a", "b"}, filepath.Join(dir, "a-events"), filepath.Join(dir, "a-inbox"), wire, time.Hour, 0, func(inbox.Message) error { return nil }, []byte("a"))
		if err != nil {
			t.Fatal(err)
		}
		if err := r.enqueue([]byte("payload")); err != nil {
			t.Fatal(err)
		}
		fmt.Println("READY")
		select {}
	}
	dir = t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestReliableSIGKILLAndLostACK$")
	cmd.Env = append(os.Environ(), "HMQ_SIGKILL_DIR="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(stdout)
		if scanner.Scan() {
			ready <- scanner.Text()
		} else {
			ready <- ""
		}
	}()
	select {
	case line := <-ready:
		if line != "READY" {
			t.Fatalf("child readiness: %q", line)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("child readiness timeout")
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("child survived SIGKILL")
	}

	aWire := runtimeSender{make(chan runtimeMessage, 100)}
	bWire := runtimeSender{make(chan runtimeMessage, 100)}
	delivered := make(chan inbox.Message, 10)
	open := func(id string, wire runtimeSender) *reliableRuntime {
		r, err := newBoundReliableRuntime(id, []string{"a", "b"}, filepath.Join(dir, id+"-events"), filepath.Join(dir, id+"-inbox"), wire, 50*time.Millisecond, 0, func(m inbox.Message) error { delivered <- m; return nil }, []byte(id))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	read := func(ch <-chan runtimeMessage) runtimeMessage {
		select {
		case m := <-ch:
			return m
		case <-time.After(3 * time.Second):
			t.Fatal("frame timeout")
			return runtimeMessage{}
		}
	}
	a, b := open("a", aWire), open("b", bWire)
	defer b.Close()
	data := read(aWire.messages)
	if _, err := b.handle("a", data.data); err != nil {
		t.Fatal(err)
	}
	_ = read(bWire.messages) // Drop the first ACK.
	select {
	case <-delivered:
	case <-time.After(3 * time.Second):
		t.Fatal("delivery timeout")
	}
	retry := read(aWire.messages)
	if _, err := b.handle("a", retry.data); err != nil {
		t.Fatal(err)
	}
	ack := read(bWire.messages)
	if _, err := a.handle("b", ack.data); err != nil {
		t.Fatal(err)
	}
	select {
	case <-delivered:
		t.Fatal("duplicate delivery")
	case <-time.After(150 * time.Millisecond):
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := eventstore.Open(filepath.Join(dir, "a-events"), eventstore.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	pending, err := store.Pending("b", 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("ACK recovery: pending=%v err=%v", pending, err)
	}
}
