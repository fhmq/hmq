package replication

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
)

func TestCheckpointRequiresEveryCursorAndFencesOldRequests(t *testing.T) {
	newPartition := func() *partition {
		return &partition{members: map[string]bool{"a": true, "b": true, "c": true},
			compact: func(previous []byte, entries []Entry) ([]byte, error) {
				var count int
				if len(previous) != 0 {
					if err := json.Unmarshal(previous, &count); err != nil {
						return nil, err
					}
				}
				return json.Marshal(count + len(entries))
			}}
	}
	p := newPartition()
	apply := func(index uint64, c command) applyResult {
		t.Helper()
		data, _ := json.Marshal(c)
		return p.Apply(&raft.Log{Index: index, Data: data}).(applyResult)
	}
	for i := 1; i <= minCompactEntries+2; i++ {
		if r := apply(uint64(i), command{ID: fmt.Sprint(i), Payload: []byte("publication")}); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	for i, id := range []string{"a", "b", "c"} {
		if r := apply(uint64(minCompactEntries+3+i), command{Progress: &Progress{Member: id, Cursor: minCompactEntries}}); r.Err != nil {
			t.Fatal(r.Err)
		}
		if i < 2 && p.floor != 0 {
			t.Fatalf("reclaimed with member %s not yet reported", []string{"b", "c"}[i])
		}
	}
	if p.floor != minCompactEntries || len(p.entries) != 2 {
		t.Fatalf("floor %d, remaining entries %d", p.floor, len(p.entries))
	}
	snap, err := p.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &testSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatal(err)
	}
	p = newPartition()
	if err := p.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(apply(999, command{ID: "1", Payload: []byte("publication")}).Err, ErrCompacted) {
		t.Fatal("compacted ID can be applied again")
	}
	if r := apply(1000, command{ID: fmt.Sprint(minCompactEntries + 1), Payload: []byte("publication")}); r.Index != minCompactEntries+1 || r.Err != nil {
		t.Fatalf("retained ID did not deduplicate: %+v", r)
	}
	if r := apply(1001, command{ID: "new", Floor: p.floor}); r.Err != nil {
		t.Fatal(r.Err)
	}
}

func TestCheckpointWALRewriteRetainsSnapshotDependency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.wal")
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StoreLogs([]*raft.Log{{Index: 1, Data: make([]byte, 64<<10)}, {Index: 2, Data: []byte("keep")}}); err != nil {
		t.Fatal(err)
	}
	backing := raft.NewInmemSnapshotStore()
	// The marker is written by the sink after successful publication.
	snapshots := checkpointSnapshots{backing, s}
	for _, index := range []uint64{1, 2} {
		sink, err := snapshots.Create(1, index, 1, raft.Configuration{}, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.DeleteRange(1, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.compact(); err != nil {
		t.Fatal(err)
	}
	if stat, err := os.Stat(path); err != nil || stat.Size() > 4096 {
		t.Fatalf("WAL did not shrink: %v %v", stat, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var entry raft.Log
	if err := s.GetLog(2, &entry); err != nil || string(entry.Data) != "keep" {
		t.Fatalf("retained log: %+v %v", entry, err)
	}
	snapshots.wal = s
	if list, err := snapshots.List(); err != nil || len(list) != 1 || list[0].Index != 2 {
		t.Fatalf("unsafe fallback: %v %v", list, err)
	}
	snapshots.SnapshotStore = raft.NewInmemSnapshotStore()
	if _, err := snapshots.List(); err == nil {
		t.Fatal("missing required snapshot did not fail closed")
	}
}
