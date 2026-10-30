package reliable_test

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/fhmq/hmq/v3/broker/eventstore"
	"github.com/fhmq/hmq/v3/broker/reliable"
	"github.com/fhmq/hmq/v3/broker/reliable/inbox"
)

type crashSender struct{}

func (crashSender) Send(string, []byte) error { return nil }

func TestProcessCrashRecovery(t *testing.T) {
	dir := os.Getenv("HMQ_CRASH_DIR")
	if dir != "" {
		events, err := eventstore.Open(filepath.Join(dir, "events"), eventstore.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		link, err := reliable.New("peer", crashSender{}, events, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if err := link.Enqueue("pending", []byte("one")); err != nil {
			t.Fatal(err)
		}
		if err := link.Enqueue("acked", []byte("two")); err != nil {
			t.Fatal(err)
		}
		ack, _ := json.Marshal(map[string]interface{}{"version": 1, "type": "ack", "id": "acked"})
		if err := link.Handle("peer", ack, nil); err != nil {
			t.Fatal(err)
		}
		in, err := inbox.Open(filepath.Join(dir, "inbox"), eventstore.Limits{})
		if err != nil {
			t.Fatal(err)
		}
		if err := in.Accept("peer", "received", []byte("three")); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // Deliberately bypass all cleanup and deferred closes.
	}
	dir = t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestProcessCrashRecovery$")
	cmd.Env = append(os.Environ(), "HMQ_CRASH_DIR="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v\n%s", err, output)
	}
	events, err := eventstore.Open(filepath.Join(dir, "events"), eventstore.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer events.Close()
	link, err := reliable.New("peer", crashSender{}, events, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer link.Close()
	if !link.Pending("pending") || link.Pending("acked") {
		t.Fatal("incorrect outgoing recovery")
	}
	in, err := inbox.Open(filepath.Join(dir, "inbox"), eventstore.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	if err := in.Accept("peer", "received", []byte("three")); err != nil {
		t.Fatal(err)
	}
	msgs, err := in.Pending("peer", 10)
	if err != nil || len(msgs) != 1 || string(msgs[0].Payload) != "three" {
		t.Fatalf("inbox: %+v, %v", msgs, err)
	}
}
