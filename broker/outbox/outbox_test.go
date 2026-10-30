package outbox

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/fhmq/hmq/v3/broker/storage"
)

func TestIncarnationRecoveryAndRecreation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	o, err := Open(path, "p", 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	inc := o.Incarnation()
	id, err := o.Enqueue([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if err = o.Close(); err != nil {
		t.Fatal(err)
	}
	o, err = Open(path, "p", 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(inc) != 32 || o.Incarnation() != inc {
		t.Fatal("incarnation changed on recovery")
	}
	pending, err := o.Pending()
	if err != nil || len(pending) != 1 || pending[0].ID != id {
		t.Fatalf("pending=%v err=%v", pending, err)
	}
	if err = o.Close(); err != nil {
		t.Fatal(err)
	}
	if err = os.Remove(path); err != nil {
		t.Fatal(err)
	}
	o, err = Open(path, "p", 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if o.Incarnation() == inc {
		t.Fatal("recreated WAL reused incarnation")
	}
}

func TestRejectOldAndInvalidHeaders(t *testing.T) {
	for _, header := range []string{
		`{"type":"peer","peer":"p"}`,
		`{"type":"peer","peer":"p","version":1,"incarnation":"bad"}`,
		`{"type":"peer","peer":"p","version":2,"incarnation":"0123456789abcdef0123456789abcdef"}`,
	} {
		l := &failingLog{records: [][]byte{[]byte(header)}}
		if _, err := recoverLog(l, "p", 2, 10); !errors.Is(err, ErrInvalid) {
			t.Fatalf("header %s: %v", header, err)
		}
	}
}

func TestRecoveryAndCapacity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	o, err := Open(path, "peer", 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	a, err := o.Enqueue([]byte("ab"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := o.Enqueue([]byte("c"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = o.Enqueue(nil); !errors.Is(err, ErrFull) {
		t.Fatal(err)
	}
	if err = o.Ack(a); err != nil {
		t.Fatal(err)
	}
	if err = o.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = Open(path, "other", 2, 3); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	o, err = Open(path, "peer", 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	p, err := o.Pending()
	if err != nil || len(p) != 1 || p[0].ID != b || string(p[0].Payload) != "c" {
		t.Fatalf("%v %v", p, err)
	}
	p[0].Payload[0] = 'x'
	p, _ = o.Pending()
	if string(p[0].Payload) != "c" {
		t.Fatal(p)
	}
	id, err := o.Enqueue(nil)
	if err != nil || id <= b {
		t.Fatal(id, err)
	}
}

func TestOversizedRecordDoesNotPoison(t *testing.T) {
	o, err := Open(filepath.Join(t.TempDir(), "wal"), "p", 2, storage.MaxRecordSize*2)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if _, err = o.Enqueue(make([]byte, storage.MaxRecordSize)); !errors.Is(err, storage.ErrTooLarge) {
		t.Fatal(err)
	}
	id, err := o.Enqueue([]byte("ok"))
	if err != nil || id != 1 {
		t.Fatalf("id=%d err=%v", id, err)
	}
}

func TestAckFailureRequiresRecovery(t *testing.T) {
	l := &failingLog{}
	o, err := recoverLog(l, "p", 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	id, err := o.Enqueue([]byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	l.fail = true
	if err = o.Ack(id); err == nil {
		t.Fatal("expected ACK failure")
	}
	l.fail = false
	if _, err = o.Pending(); err == nil {
		t.Fatal("expected poisoned instance")
	}
	_ = o.Close()
	o, err = recoverLog(l, "p", 2, 10)
	if err != nil {
		t.Fatal(err)
	}
	p, err := o.Pending()
	if err != nil || len(p) != 1 || p[0].ID != id {
		t.Fatalf("pending=%v err=%v", p, err)
	}
	if err = o.Ack(id); err != nil {
		t.Fatal(err)
	}
	if err = o.Ack(id); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}
func TestConcurrent(t *testing.T) {
	o, err := Open(filepath.Join(t.TempDir(), "wal"), "p", 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, e := o.Enqueue([]byte("x"))
			if e != nil {
				t.Error(e)
				return
			}
			if e = o.Ack(id); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	p, err := o.Pending()
	if err != nil || len(p) != 0 {
		t.Fatal(p, err)
	}
}

type failingLog struct {
	records [][]byte
	fail    bool
}

func (l *failingLog) Append(b []byte) error {
	if l.fail {
		return errors.New("write failed")
	}
	l.records = append(l.records, append([]byte(nil), b...))
	return nil
}
func (l *failingLog) Replay(f func([]byte) error) error {
	for _, b := range l.records {
		if e := f(b); e != nil {
			return e
		}
	}
	return nil
}
func (l *failingLog) Close() error { return nil }
func TestFailurePoisonsInstance(t *testing.T) {
	l := &failingLog{}
	o, e := recoverLog(l, "p", 2, 10)
	if e != nil {
		t.Fatal(e)
	}
	l.fail = true
	if _, e = o.Enqueue([]byte("x")); e == nil {
		t.Fatal("expected failure")
	}
	l.fail = false
	if _, e = o.Pending(); e == nil {
		t.Fatal("expected poisoned instance")
	}
	o.Close()
	o, e = recoverLog(l, "p", 2, 10)
	if e != nil {
		t.Fatal(e)
	}
	p, e := o.Pending()
	if e != nil || len(p) != 0 {
		t.Fatal(p, e)
	}
}
