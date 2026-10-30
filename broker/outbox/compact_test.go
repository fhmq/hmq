package outbox

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCompactSnapshotRejectsInvalidHistory(t *testing.T) {
	for _, tc := range []struct {
		name    string
		next    uint64
		records []record
	}{
		{"zero-high-water", 0, nil},
		{"pending-at-high-water", 3, []record{{Type: "pending", ID: 3}}},
		{"duplicate", 3, []record{{Type: "pending", ID: 1}, {Type: "pending", ID: 1}}},
		{"out-of-order", 3, []record{{Type: "pending", ID: 2}, {Type: "pending", ID: 1}}},
		{"snapshot-after-append", 3, []record{{Type: "enqueue", ID: 3}, {Type: "pending", ID: 1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := &failingLog{}
			records := append([]record{{Type: "peer", Peer: "p", Version: 2, Incarnation: "00000000000000000000000000000001", ID: tc.next}}, tc.records...)
			for _, r := range records {
				data, err := json.Marshal(r)
				if err != nil {
					t.Fatal(err)
				}
				l.records = append(l.records, data)
			}
			if _, err := recoverLog(l, "p", 100, 1000); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid snapshot accepted: %v", err)
			}
		})
	}
}

func TestCompactPreservesIdentityHighWaterAndPending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "outbox")
	o, err := Open(path, "peer", 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	identity := o.Incarnation()
	for i := 1; i <= 50; i++ {
		id, err := o.Enqueue([]byte("payload"))
		if err != nil || id != uint64(i) {
			t.Fatalf("enqueue: %d %v", id, err)
		}
		if i != 25 && i != 30 && i != 45 {
			if err := o.Ack(id); err != nil {
				t.Fatal(err)
			}
		}
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Compact(); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil || after.Size() >= before.Size() {
		t.Fatalf("not compacted: %v", err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o, err = Open(path, "peer", 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if o.Incarnation() != identity {
		t.Fatal("incarnation changed")
	}
	if len(o.pending) != 3 || string(o.pending[25]) != "payload" || string(o.pending[30]) != "payload" || string(o.pending[45]) != "payload" {
		t.Fatalf("pending: %v", o.pending)
	}
	if !reflect.DeepEqual(o.order, []uint64{25, 30, 45}) {
		t.Fatalf("pending order: %v", o.order)
	}
	for _, id := range []uint64{25, 30, 45} {
		if err := o.Ack(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := o.Compact(); err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o, err = Open(path, "peer", 100, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	id, err := o.Enqueue([]byte("later"))
	if err != nil || id != 51 {
		t.Fatalf("ID reused: %d %v", id, err)
	}
	if err := o.Compact(); err != nil {
		t.Fatal(err)
	}
}
