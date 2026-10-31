package replication

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/fhmq/hmq/v4/broker/storage"
	"github.com/hashicorp/raft"
)

func openTestStore(t *testing.T, path string) *Store {
	t.Helper()
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func mustStore(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func bounds(t *testing.T, s *Store, first, last uint64) {
	t.Helper()
	f, err := s.FirstIndex()
	mustStore(t, err)
	l, err := s.LastIndex()
	mustStore(t, err)
	if f != first || l != last {
		t.Fatalf("bounds %d,%d want %d,%d", f, l, first, last)
	}
}

func TestStoreRestartAndOwnership(t *testing.T) {
	p := filepath.Join(t.TempDir(), "raft.wal")
	s := openTestStore(t, p)
	bounds(t, s, 0, 0)
	if v, err := s.Get([]byte("absent")); err != nil || len(v) != 0 {
		t.Fatalf("missing: %v %v", v, err)
	}
	if v, err := s.GetUint64([]byte("absent")); err != nil || v != 0 {
		t.Fatalf("missing uint: %v %v", v, err)
	}
	want := raft.Log{Index: 4, Term: 8, Type: raft.LogConfiguration, Data: []byte{0, 255}, Extensions: []byte("ext"), AppendedAt: time.Date(2026, 1, 2, 3, 4, 5, 678, time.UTC)}
	in := copyLog(want)
	mustStore(t, s.StoreLogs([]*raft.Log{&in, {Index: 9, Term: 8}}))
	in.Data[0] = 9
	in.Extensions[0] = 'x'
	in.Index = 1
	key, val := []byte{0, 255}, []byte("vote")
	mustStore(t, s.Set(key, val))
	key[0] = 1
	val[0] = 'X'
	for _, k := range []string{"CurrentTerm", "LastVoteTerm"} {
		mustStore(t, s.SetUint64([]byte(k), 42))
	}
	mustStore(t, s.Set([]byte("LastVoteCand"), []byte("node-b")))
	check := func() {
		bounds(t, s, 4, 9)
		var got raft.Log
		mustStore(t, s.GetLog(4, &got))
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("log: %#v want %#v", got, want)
		}
		got.Data[0] = 77
		got.Extensions[0] = 'z'
		v, err := s.Get([]byte{0, 255})
		mustStore(t, err)
		if string(v) != "vote" {
			t.Fatalf("value: %q", v)
		}
		v[0] = 'Y'
		for _, k := range []string{"CurrentTerm", "LastVoteTerm"} {
			v, err := s.GetUint64([]byte(k))
			mustStore(t, err)
			if v != 42 {
				t.Fatal(v)
			}
		}
		v, err = s.Get([]byte("LastVoteCand"))
		mustStore(t, err)
		if string(v) != "node-b" {
			t.Fatal(string(v))
		}
		if !errors.Is(s.GetLog(5, &got), raft.ErrLogNotFound) {
			t.Fatal("missing log")
		}
	}
	check()
	check()
	mustStore(t, s.Close())
	s = openTestStore(t, p)
	check()
	mustStore(t, s.StoreLog(&raft.Log{Index: 4, Term: 10}))
	mustStore(t, s.Close())
	s = openTestStore(t, p)
	var got raft.Log
	mustStore(t, s.GetLog(4, &got))
	if got.Term != 10 {
		t.Fatal(got)
	}
}

func TestStoreDeleteRange(t *testing.T) {
	p := filepath.Join(t.TempDir(), "raft.wal")
	s := openTestStore(t, p)
	for _, i := range []uint64{1, 3, 7, ^uint64(0)} {
		mustStore(t, s.StoreLog(&raft.Log{Index: i}))
	}
	mustStore(t, s.DeleteRange(2, 6))
	bounds(t, s, 1, ^uint64(0))
	mustStore(t, s.DeleteRange(0, 1))
	bounds(t, s, 7, ^uint64(0))
	mustStore(t, s.DeleteRange(^uint64(0), ^uint64(0)))
	bounds(t, s, 7, 7)
	mustStore(t, s.Close())
	s = openTestStore(t, p)
	bounds(t, s, 7, 7)
	if !errors.Is(s.DeleteRange(9, 8), ErrStoreInvalid) {
		t.Fatal("reversed range")
	}
	mustStore(t, s.DeleteRange(0, ^uint64(0)))
	bounds(t, s, 0, 0)
	mustStore(t, s.Close())
	s = openTestStore(t, p)
	bounds(t, s, 0, 0)
}

func TestStoreBatchAtomicValidation(t *testing.T) {
	p := filepath.Join(t.TempDir(), "raft.wal")
	s := openTestStore(t, p)
	for _, batch := range [][]*raft.Log{{{Index: 1}, nil}, {{Index: 1}, {Index: 0}}} {
		if !errors.Is(s.StoreLogs(batch), ErrStoreInvalid) {
			t.Fatal("invalid batch accepted")
		}
		bounds(t, s, 0, 0)
	}
	large := &raft.Log{Index: 2, Data: make([]byte, storage.MaxRecordSize)}
	if !errors.Is(s.StoreLogs([]*raft.Log{{Index: 1}, large}), storage.ErrTooLarge) {
		t.Fatal("oversized batch accepted")
	}
	bounds(t, s, 0, 0)
	mustStore(t, s.StoreLogs([]*raft.Log{{Index: 3}, {Index: 5}}))
	w, err := s.wal.(*storage.Log).ReadAll()
	mustStore(t, err)
	if len(w) != 1 {
		t.Fatalf("batch split across %d WAL records", len(w))
	}
	mustStore(t, s.Close())
	s = openTestStore(t, p)
	bounds(t, s, 3, 5)
}

type failingStoreWAL struct {
	storeWAL
	persist bool
	calls   int
}

var errStoreWrite = errors.New("injected sync failure")

func (w *failingStoreWAL) Append(b []byte) error {
	w.calls++
	if w.persist {
		if err := w.storeWAL.Append(b); err != nil {
			return err
		}
	}
	return errStoreWrite
}

func TestStorePoison(t *testing.T) {
	for _, persist := range []bool{false, true} {
		for _, op := range []string{"logs", "set", "uint", "delete"} {
			t.Run(fmt.Sprintf("%s/persist=%t", op, persist), func(t *testing.T) {
				p := filepath.Join(t.TempDir(), "raft.wal")
				s := openTestStore(t, p)
				mustStore(t, s.StoreLog(&raft.Log{Index: 1}))
				w := &failingStoreWAL{storeWAL: s.wal, persist: persist}
				s.wal = w
				var err error
				switch op {
				case "logs":
					err = s.StoreLogs([]*raft.Log{{Index: 2}, {Index: 3}})
				case "set":
					err = s.Set([]byte("vote"), []byte("b"))
				case "uint":
					err = s.SetUint64([]byte("term"), 5)
				case "delete":
					err = s.DeleteRange(1, 1)
				}
				if !errors.Is(err, errStoreWrite) {
					t.Fatal(err)
				}
				assertStoreUnavailable(t, s)
				if w.calls != 1 {
					t.Fatalf("poisoned store wrote again: %d", w.calls)
				}
				mustStore(t, s.Close())
				s = openTestStore(t, p)
				if op == "logs" && persist {
					bounds(t, s, 1, 3)
				} else if op == "delete" && persist {
					bounds(t, s, 0, 0)
				} else {
					bounds(t, s, 1, 1)
				}
			})
		}
	}
}

func assertStoreUnavailable(t *testing.T, s *Store) {
	t.Helper()
	_, e1 := s.FirstIndex()
	_, e2 := s.LastIndex()
	_, e3 := s.Get(nil)
	_, e4 := s.GetUint64(nil)
	for _, err := range []error{e1, e2, e3, e4, s.GetLog(1, new(raft.Log)), s.StoreLogs(nil), s.StoreLog(&raft.Log{Index: 9}), s.Set(nil, nil), s.SetUint64(nil, 1), s.DeleteRange(0, 0)} {
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("operation after poison/close: %v", err)
		}
	}
}

func TestStoreLockCorruptionAndClose(t *testing.T) {
	p := filepath.Join(t.TempDir(), "raft.wal")
	s := openTestStore(t, p)
	if other, err := OpenStore(p); err == nil {
		other.Close()
		t.Fatal("second owner accepted")
	}
	mustStore(t, s.Close())
	assertStoreUnavailable(t, s)
	if !errors.Is(s.Close(), ErrUnavailable) {
		t.Fatal("second close")
	}
	mustStore(t, os.WriteFile(p, []byte("torn"), 0600))
	if _, err := OpenStore(p); !errors.Is(err, storage.ErrCorrupt) {
		t.Fatal(err)
	}
	// Valid WAL framing cannot make an unknown adapter record acceptable.
	mustStore(t, os.Truncate(p, 0))
	w, err := storage.Open(p)
	mustStore(t, err)
	mustStore(t, w.Append([]byte(`{"Version":99,"Op":"set"}`)))
	mustStore(t, w.Close())
	if _, err := OpenStore(p); !errors.Is(err, ErrStoreInvalid) {
		t.Fatal(err)
	}
	w, err = storage.Open(p)
	mustStore(t, err)
	mustStore(t, w.Close())
}

func TestStoreConcurrent(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "raft.wal"))
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				i := uint64(g*20 + j + 1)
				key := []byte(fmt.Sprint(i))
				if err := s.StoreLog(&raft.Log{Index: i, Data: key}); err != nil {
					t.Error(err)
					return
				}
				if err := s.Set(key, key); err != nil {
					t.Error(err)
					return
				}
				var l raft.Log
				if err := s.GetLog(i, &l); err != nil {
					t.Error(err)
					return
				}
				v, err := s.Get(key)
				if err != nil || !bytes.Equal(v, key) {
					t.Errorf("get %q: %v", v, err)
				}
				s.FirstIndex()
				s.LastIndex()
			}
		}(g)
	}
	wg.Wait()
	bounds(t, s, 1, 160)
}

func TestStoreTornBatchFailsClosed(t *testing.T) {
	p := filepath.Join(t.TempDir(), "raft.wal")
	s := openTestStore(t, p)
	mustStore(t, s.StoreLog(&raft.Log{Index: 1}))
	info, err := os.Stat(p)
	mustStore(t, err)
	start := int(info.Size())
	mustStore(t, s.StoreLogs([]*raft.Log{{Index: 2}, {Index: 3}}))
	mustStore(t, s.Close())
	data, err := os.ReadFile(p)
	mustStore(t, err)
	for _, n := range []int{start, start + 1, start + 16, len(data) - 1, len(data)} {
		path := filepath.Join(t.TempDir(), "cut.wal")
		mustStore(t, os.WriteFile(path, data[:n], 0600))
		got, err := OpenStore(path)
		if n != start && n != len(data) {
			if got != nil {
				got.Close()
			}
			if !errors.Is(err, storage.ErrCorrupt) {
				t.Fatalf("cut %d: %v", n, err)
			}
			continue
		}
		mustStore(t, err)
		if n == start {
			bounds(t, got, 1, 1)
		} else {
			bounds(t, got, 1, 3)
		}
		mustStore(t, got.Close())
	}
}

func TestStoreUint64Encoding(t *testing.T) {
	s := openTestStore(t, filepath.Join(t.TempDir(), "raft.wal"))
	mustStore(t, s.SetUint64([]byte("term"), ^uint64(0)))
	v, err := s.Get([]byte("term"))
	mustStore(t, err)
	if !bytes.Equal(v, bytes.Repeat([]byte{255}, 8)) {
		t.Fatalf("uint encoding: %x", v)
	}
	mustStore(t, s.Set([]byte("bad"), []byte{1}))
	if _, err := s.GetUint64([]byte("bad")); !errors.Is(err, ErrStoreInvalid) {
		t.Fatal(err)
	}
	if err := s.GetLog(1, nil); !errors.Is(err, ErrStoreInvalid) {
		t.Fatal(err)
	}
}
