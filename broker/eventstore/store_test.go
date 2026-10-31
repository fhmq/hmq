package eventstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/fhmq/hmq/v4/broker/storage"
)

func TestRecoveryAndDedup(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events")
	s, err := Open(path, Limits{MaxEvents: 1, MaxBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Put("i", []byte("p"), []string{"b", "a"}); err != nil {
		t.Fatal(err)
	}
	if err = s.Ack("i", "a"); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Limits{MaxEvents: 1, MaxBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.Put("i", []byte("p"), []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(s.Put("i", []byte("q"), []string{"a", "b"}), ErrConflict) {
		t.Fatal("expected conflict")
	}
	if !errors.Is(s.Put("j", nil, []string{"a"}), ErrCapacity) {
		t.Fatal("expected capacity")
	}
	if err = s.Ack("i", "a"); err != nil {
		t.Fatal(err)
	}
	e, err := s.Get("i")
	if err != nil || !reflect.DeepEqual(e.Confirmed, []string{"a"}) {
		t.Fatalf("%+v %v", e, err)
	}
	e.Targets[0] = "changed"
	e.Payload[0] = 'x'
	e, _ = s.Get("i")
	if string(e.Payload) != "p" || e.Targets[0] != "a" {
		t.Fatal("aliased data")
	}
}

func TestPendingRecoveryAndCopies(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events")
	s, err := Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"z", "a", "m"} {
		if err := s.Put(id, []byte(id), []string{"other", "target"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put("a", []byte("a"), []string{"target", "other"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Ack("a", "target"); err != nil {
		t.Fatal(err)
	}
	if err := s.Ack("z", "other"); err != nil {
		t.Fatal(err)
	}
	check := func() {
		t.Helper()
		got, err := s.Pending("target", 1)
		if err != nil || len(got) != 1 || got[0].ID != "z" {
			t.Fatalf("bounded: %+v %v", got, err)
		}
		got[0].Payload[0] = 'x'
		got[0].Targets[0] = "x"
		got[0].Confirmed[0] = "x"
		got, err = s.Pending("target", 10)
		if err != nil || len(got) != 2 || got[0].ID != "z" || got[1].ID != "m" || string(got[0].Payload) != "z" || got[0].Targets[0] != "other" || got[0].Confirmed[0] != "other" {
			t.Fatalf("pending: %+v %v", got, err)
		}
		got, err = s.Pending("missing", 1)
		if err != nil || len(got) != 0 {
			t.Fatalf("missing: %+v %v", got, err)
		}
	}
	check()
	for _, tc := range []struct {
		target string
		limit  int
	}{{"", 1}, {string([]byte{0xff}), 1}, {"target", 0}, {"target", -1}} {
		if _, err := s.Pending(tc.target, tc.limit); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid: %v", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pending("target", 1); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	s, err = Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	check()
}

func TestPendingConcurrent(t *testing.T) {
	s, err := recoverStore(&failingLog{}, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				id := fmt.Sprintf("%d-%d", worker, i)
				if err := s.Put(id, []byte(id), []string{"target"}); err != nil {
					t.Error(err)
					return
				}
				if _, err := s.Pending("target", 5); err != nil {
					t.Error(err)
					return
				}
				if err := s.Ack(id, "target"); err != nil {
					t.Error(err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	got, err := s.Pending("target", 5)
	if err != nil || len(got) != 0 {
		t.Fatalf("pending: %+v %v", got, err)
	}
}

type failingLog struct {
	records [][]byte
	fail    bool
}

func (l *failingLog) Append(b []byte) error {
	l.records = append(l.records, append([]byte(nil), b...))
	if l.fail {
		return errors.New("uncertain sync")
	}
	return nil
}
func (l *failingLog) Replay(f func([]byte) error) error {
	for _, b := range l.records {
		if err := f(b); err != nil {
			return err
		}
	}
	return nil
}
func (l *failingLog) Close() error { return nil }
func TestUncertainAppend(t *testing.T) {
	l := &failingLog{fail: true}
	s, _ := recoverStore(l, Limits{})
	if s.Put("x", nil, []string{"a"}) == nil {
		t.Fatal("expected failure")
	}
	if _, err := s.Get("x"); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	l.fail = false
	s, err := recoverStore(l, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Get("x"); err != nil {
		t.Fatal(err)
	}
}
func TestIncompleteTailRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	s, err := Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Put("x", nil, []string{"a"}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.Write([]byte("HMQ")); err != nil {
		t.Fatal(err)
	}
	f.Close()
	_, err = Open(path, Limits{})
	if !errors.Is(err, storage.ErrCorrupt) {
		t.Fatal(err)
	}
}

func TestInvalidIdentifiersDoNotPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events")
	s, err := Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	bad := string([]byte{0xff})
	for _, tc := range []struct {
		id      string
		targets []string
	}{
		{bad, []string{"target"}}, {"id", []string{bad}},
	} {
		if err := s.Put(tc.id, nil, tc.targets); !errors.Is(err, ErrInvalid) {
			t.Fatalf("Put: %v", err)
		}
	}
	if err := s.Put("id", nil, []string{"target"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Ack("id", bad); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Ack: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Get("id"); err != nil {
		t.Fatal(err)
	}
}

func TestRecoveryRejectsInvalidHistoryAndCapacity(t *testing.T) {
	for _, records := range [][][]byte{
		{[]byte("{\"type\":\"event\",\"id\":\"\xff\",\"targets\":[\"a\"]}")},
		{[]byte(`{"type":"ack","id":"x","target":"a"}`)},
		{[]byte(`{"type":"event","id":"x","targets":["a"]}`), []byte(`{"type":"ack","id":"x","target":"b"}`)},
		{[]byte(`{"type":"event","id":"x","targets":["a"]}`), []byte(`{"type":"ack","id":"x","target":"a"}`), []byte(`{"type":"ack","id":"x","target":"a"}`)},
	} {
		if _, err := recoverStore(&failingLog{records: records}, Limits{}); !errors.Is(err, ErrInvalid) {
			t.Fatalf("history %q: %v", records, err)
		}
	}
	l := &failingLog{records: [][]byte{[]byte(`{"type":"event","id":"x","payload":"cA==","targets":["a"]}`)}}
	if _, err := recoverStore(l, Limits{MaxBytes: 2}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("capacity: %v", err)
	}
	if _, err := recoverStore(l, Limits{MaxBytes: 3}); err != nil {
		t.Fatal(err)
	}
}

func TestUncertainAckRecovery(t *testing.T) {
	l := &failingLog{}
	s, _ := recoverStore(l, Limits{})
	if err := s.Put("x", nil, []string{"a"}); err != nil {
		t.Fatal(err)
	}
	l.fail = true
	if err := s.Ack("x", "a"); err == nil {
		t.Fatal("expected failure")
	}
	if err := s.Ack("x", "a"); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	l.fail = false
	s, err := recoverStore(l, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Ack("x", "a"); err != nil {
		t.Fatal(err)
	}
	e, err := s.Get("x")
	if err != nil || !reflect.DeepEqual(e.Confirmed, []string{"a"}) {
		t.Fatalf("%+v %v", e, err)
	}
}

func TestMultiTargetAppendFailureRecovery(t *testing.T) {
	for _, persisted := range []bool{false, true} {
		t.Run(fmt.Sprint(persisted), func(t *testing.T) {
			l := &failingLog{fail: true}
			s, _ := recoverStore(l, Limits{})
			if err := s.Put("x", []byte("p"), []string{"b", "a"}); err == nil {
				t.Fatal("expected append failure")
			}
			if _, err := s.Pending("a", 1); !errors.Is(err, ErrUnavailable) {
				t.Fatal(err)
			}
			if !persisted {
				l.records = nil
			}
			l.fail = false
			s, err := recoverStore(l, Limits{})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Put("x", []byte("p"), []string{"a", "b"}); err != nil {
				t.Fatal(err)
			}
			for _, target := range []string{"a", "b"} {
				pending, err := s.Pending(target, 1)
				if err != nil || len(pending) != 1 || !reflect.DeepEqual(pending[0].Targets, []string{"a", "b"}) {
					t.Fatalf("%s: %+v %v", target, pending, err)
				}
			}
		})
	}
}

func TestMultiTargetUncertainAck(t *testing.T) {
	l := &failingLog{}
	s, _ := recoverStore(l, Limits{})
	if err := s.Put("x", nil, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	l.fail = true
	if err := s.Ack("x", "a"); err == nil {
		t.Fatal("expected failure")
	}
	l.fail = false
	s, err := recoverStore(l, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.Pending("a", 1)
	if err != nil || len(a) != 0 {
		t.Fatalf("a: %+v %v", a, err)
	}
	b, err := s.Pending("b", 1)
	if err != nil || len(b) != 1 {
		t.Fatalf("b: %+v %v", b, err)
	}
}
