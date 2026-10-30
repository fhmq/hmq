package replication

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/hashicorp/raft"
)

type testSink struct {
	bytes.Buffer
	closed, cancelled bool
}

func (*testSink) ID() string      { return "test" }
func (s *testSink) Close() error  { s.closed = true; return nil }
func (s *testSink) Cancel() error { s.cancelled = true; return nil }

func TestPartitionCursorAcrossLogGaps(t *testing.T) {
	p := &partition{}
	for i, id := range []string{"a", "b", "c"} {
		data, _ := json.Marshal(command{id, []byte(id)})
		if r := p.Apply(&raft.Log{Index: uint64(i*10 + 5), Data: data}).(applyResult); r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	for _, tc := range []struct {
		after uint64
		want  string
	}{{0, "a"}, {5, "b"}, {14, "b"}, {15, "c"}, {25, ""}, {^uint64(0), ""}} {
		got := p.entriesAfter(tc.after, 1)
		if tc.want == "" {
			if len(got) != 0 {
				t.Fatalf("after %d: %v", tc.after, got)
			}
			continue
		}
		if len(got) != 1 || got[0].ID != tc.want {
			t.Fatalf("after %d: %v", tc.after, got)
		}
	}
}

func TestPartitionDedupSnapshotAndInvalidRestore(t *testing.T) {
	p := &partition{}
	apply := func(index uint64, id, payload string) applyResult {
		data, err := json.Marshal(command{id, []byte(payload)})
		if err != nil {
			t.Fatal(err)
		}
		return p.Apply(&raft.Log{Index: index, Data: data}).(applyResult)
	}
	if r := apply(2, "one", "payload"); r.Err != nil || r.Index != 2 {
		t.Fatal(r)
	}
	if r := apply(3, "one", "payload"); r.Err != nil || r.Index != 2 {
		t.Fatal(r)
	}
	if r := apply(4, "one", "changed"); !errors.Is(r.Err, ErrConflict) {
		t.Fatal(r)
	}
	snapshot, err := p.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	sink := &testSink{}
	if err := snapshot.Persist(sink); err != nil || !sink.closed || sink.cancelled {
		t.Fatalf("snapshot: %v %+v", err, sink)
	}
	restored := &partition{}
	if err := restored.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	entries := restored.entriesAfter(0, 10)
	if len(entries) != 1 || entries[0].Index != 2 || string(entries[0].Payload) != "payload" {
		t.Fatal(entries)
	}
	entries[0].Payload[0] = 'x'
	if string(restored.entriesAfter(0, 10)[0].Payload) != "payload" {
		t.Fatal("aliased read")
	}
	if err := restored.Restore(io.NopCloser(bytes.NewBufferString(`{"version":1,"entries":[{"index":0,"id":"bad"}]}`))); err == nil {
		t.Fatal("accepted invalid snapshot")
	}
	if len(restored.entriesAfter(0, 10)) != 1 {
		t.Fatal("failed restore changed state")
	}
}
