package eventstore

import (
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"strconv"
	"testing"
)

func TestPendingAfterACKedCursorAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events")
	s, err := Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b", "c"} {
		if err := s.Put(id, nil, []string{"target"}); err != nil {
			t.Fatal(err)
		}
	}
	page, cursor, err := s.PendingAfter("target", 0, 2)
	if err != nil || len(page) != 2 || page[1].ID != "b" {
		t.Fatalf("first page: %+v, %v", page, err)
	}
	if err := s.Ack("b", "target"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	page, _, err = s.PendingAfter("target", cursor, 2)
	if err != nil || len(page) != 1 || page[0].ID != "c" {
		t.Fatalf("next page: %+v, %v", page, err)
	}
}

func TestPendingUntilSnapshot(t *testing.T) {
	s, err := recoverStore(&failingLog{}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a", "b"} {
		if err := s.Put(id, nil, []string{"target"}); err != nil {
			t.Fatal(err)
		}
	}
	through, err := s.Sequence()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put("c", nil, []string{"target"}); err != nil {
		t.Fatal(err)
	}
	page, cursor, err := s.PendingUntil("target", 0, through, 1)
	if err != nil || len(page) != 1 || page[0].ID != "a" {
		t.Fatalf("first page: %+v %v", page, err)
	}
	if err := s.Ack("a", "target"); err != nil {
		t.Fatal(err)
	}
	page, cursor, err = s.PendingUntil("target", cursor, through, 10)
	if err != nil || len(page) != 1 || page[0].ID != "b" {
		t.Fatalf("bounded page: %+v %v", page, err)
	}
	page, next, err := s.PendingUntil("target", cursor, through, 10)
	if err != nil || len(page) != 0 || next != cursor {
		t.Fatalf("bounded end: %+v %d %v", page, next, err)
	}
}

func checkPendingTree(t *testing.T, n *pendingNode, low, high uint64) (int, int) {
	t.Helper()
	if n == nil {
		return 0, 0
	}
	if n.sequence <= low || n.sequence >= high {
		t.Fatalf("out-of-order sequence %d", n.sequence)
	}
	lh, lc := checkPendingTree(t, n.left, low, n.sequence)
	rh, rc := checkPendingTree(t, n.right, n.sequence, high)
	if lh-rh > 1 || rh-lh > 1 {
		t.Fatalf("unbalanced at %d: %d/%d", n.sequence, lh, rh)
	}
	if rh > lh {
		lh = rh
	}
	if n.height != lh+1 {
		t.Fatalf("incorrect height at %d", n.sequence)
	}
	return lh + 1, lc + rc + 1
}

func TestLargePendingIndexPaginationAndRecovery(t *testing.T) {
	const total = 32768
	l := &failingLog{}
	s, err := recoverStore(l, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < total; i++ {
		if err := s.Put(strconv.Itoa(i), nil, []string{"a", "b"}); err != nil {
			t.Fatal(err)
		}
	}
	// Delete interleaved sequences in both targets, including page boundaries.
	for i := 0; i < total; i++ {
		if i%3 == 0 {
			if err := s.Ack(strconv.Itoa(i), "a"); err != nil {
				t.Fatal(err)
			}
		}
		if i%5 == 0 {
			if err := s.Ack(strconv.Itoa(i), "b"); err != nil {
				t.Fatal(err)
			}
		}
	}
	verify := func(s *Store) {
		for _, tc := range []struct {
			target  string
			divisor int
		}{{"a", 3}, {"b", 5}} {
			q := s.pending[tc.target]
			_, count := checkPendingTree(t, q.root, 0, ^uint64(0))
			if count != q.items.Len() {
				t.Fatalf("%s tree=%d list=%d", tc.target, count, q.items.Len())
			}
			var cursor uint64
			want := 0
			for {
				page, next, err := s.PendingAfter(tc.target, cursor, 127)
				if err != nil {
					t.Fatal(err)
				}
				if len(page) == 0 {
					break
				}
				for _, e := range page {
					for want < total && want%tc.divisor == 0 {
						want++
					}
					if e.ID != strconv.Itoa(want) {
						t.Fatalf("%s got %s want %d", tc.target, e.ID, want)
					}
					want++
				}
				cursor = next
			}
			for want < total && want%tc.divisor == 0 {
				want++
			}
			if want != total {
				t.Fatalf("%s ended at %d", tc.target, want)
			}
		}
	}
	verify(s)
	page, cursor, err := s.PendingAfter("a", 0, 127)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Ack(page[len(page)-1].ID, "a"); err != nil {
		t.Fatal(err)
	}
	next, _, err := s.PendingAfter("a", cursor, 1)
	if err != nil || len(next) != 1 {
		t.Fatalf("after deleted cursor: %+v %v", next, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = recoverStore(l, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	after, _, err := s.PendingAfter("a", cursor, 1)
	if err != nil || len(after) != 1 || after[0].ID != next[0].ID {
		t.Fatalf("after recovery: %+v %v", after, err)
	}
	for _, q := range s.pending {
		checkPendingTree(t, q.root, 0, ^uint64(0))
	}
}

func BenchmarkPendingAfterBacklog(b *testing.B) {
	for _, total := range []int{4096, 65536, 262144} {
		b.Run(strconv.Itoa(total), func(b *testing.B) {
			s, err := recoverStore(&failingLog{}, Limits{})
			if err != nil {
				b.Fatal(err)
			}
			for i := 0; i < total; i++ {
				if err := s.Put(strconv.Itoa(i), nil, []string{"peer"}); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var cursor uint64
				for {
					page, next, err := s.PendingAfter("peer", cursor, 256)
					if err != nil {
						b.Fatal(err)
					}
					if len(page) == 0 {
						break
					}
					cursor = next
				}
			}
		})
	}
}

func TestPendingIndexRandomACKAndAppend(t *testing.T) {
	const total = 4096
	s, err := recoverStore(&failingLog{}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < total; i++ {
		if err := s.Put(strconv.Itoa(i), nil, []string{"peer"}); err != nil {
			t.Fatal(err)
		}
	}
	order := rand.New(rand.NewSource(42)).Perm(total)
	for i, id := range order {
		if err := s.Ack(strconv.Itoa(id), "peer"); err != nil {
			t.Fatal(err)
		}
		if i%73 == 0 {
			q := s.pending["peer"]
			_, count := checkPendingTree(t, q.root, 0, ^uint64(0))
			if count != total-i-1 || count != q.items.Len() {
				t.Fatalf("tree=%d list=%d after %d ACKs", count, q.items.Len(), i+1)
			}
		}
	}
	if s.pending["peer"] != nil {
		t.Fatal("empty target retained")
	}
	if err := s.Put("new", nil, []string{"peer"}); err != nil {
		t.Fatal(err)
	}
	page, _, err := s.PendingAfter("peer", total, 1)
	if err != nil || len(page) != 1 || page[0].ID != "new" {
		t.Fatalf("appended after old cursor: %+v, %v", page, err)
	}
}

func TestPendingIndexRebuildPreservesDedup(t *testing.T) {
	l := &failingLog{}
	s, err := recoverStore(l, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1200; i++ {
		id := fmt.Sprint(i)
		if err := s.Put(id, []byte("payload"), []string{"a", "b"}); err != nil {
			t.Fatal(err)
		}
		if err := s.Ack(id, "a"); err != nil {
			t.Fatal(err)
		}
		if i != 5 && i != 1100 {
			if err := s.Ack(id, "b"); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = recoverStore(l, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if s.pending["a"] != nil {
		t.Fatal("confirmed target retained in pending index")
	}
	p, err := s.Pending("b", 10)
	if err != nil || len(p) != 2 || p[0].ID != "5" || p[1].ID != "1100" {
		t.Fatalf("pending = %+v, error = %v", p, err)
	}
	if err := s.Put("0", []byte("payload"), []string{"b", "a"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Put("0", []byte("different"), []string{"a", "b"}); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict = %v", err)
	}
	if err := s.Ack("5", "b"); err != nil {
		t.Fatal(err)
	}
	p, err = s.Pending("b", 10)
	if err != nil || len(p) != 1 || p[0].ID != "1100" {
		t.Fatalf("pending after ack = %+v, error = %v", p, err)
	}
}
