package replication

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
)

func TestMaximumProposalBatchFitsWAL(t *testing.T) {
	s, err := OpenStore(filepath.Join(t.TempDir(), "raft.wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	data, err := json.Marshal(command{ID: "maximum", Payload: make([]byte, MaxPayloadSize)})
	if err != nil {
		t.Fatal(err)
	}
	var logs []*raft.Log
	for i := 0; i < maxAppendBatch; i++ {
		logs = append(logs, &raft.Log{Index: uint64(i + 1), Term: 1, Type: raft.LogCommand, Data: data})
	}
	if err := s.StoreLogs(logs); err != nil {
		t.Fatalf("maximum legal batch does not fit WAL: %v", err)
	}
}
