package inbox

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/fhmq/hmq/v4/broker/storage"
)

func TestRestartDuplicateAndGap(t *testing.T) {
	p := filepath.Join(t.TempDir(), "inbox")
	i, err := Open(p, "sender")
	if err != nil {
		t.Fatal(err)
	}
	if ok, err := i.Accept(2); ok || !errors.Is(err, ErrGap) {
		t.Fatalf("gap: %v %v", ok, err)
	}
	if ok, err := i.Accept(0); ok || !errors.Is(err, ErrInvalid) {
		t.Fatalf("zero: %v %v", ok, err)
	}
	if ok, err := i.Accept(1); !ok || err != nil {
		t.Fatalf("accept: %v %v", ok, err)
	}
	if err := i.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(p, "other"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("sender: %v", err)
	}
	i, err = Open(p, "sender")
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	if ok, err := i.Accept(1); ok || err != nil {
		t.Fatalf("duplicate: %v %v", ok, err)
	}
	if ok, err := i.Accept(2); !ok || err != nil {
		t.Fatalf("next: %v %v", ok, err)
	}
}

func TestEventsRecoveryAndCompletion(t *testing.T) {
	p := filepath.Join(t.TempDir(), "log")
	i, err := Open(p, "s")
	if err != nil {
		t.Fatal(err)
	}
	for id := uint64(1); id <= 3; id++ {
		if ok, err := i.AcceptEvent(id, []byte{byte(id)}); !ok || err != nil {
			t.Fatal(ok, err)
		}
	}
	if err := i.Complete(2); err != nil {
		t.Fatal(err)
	}
	if err := i.Close(); err != nil {
		t.Fatal(err)
	}
	i, err = Open(p, "s")
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	events, err := i.Pending()
	if err != nil || len(events) != 2 || events[0].ID != 1 || events[1].ID != 3 {
		t.Fatal(events, err)
	}
	events[0].Payload[0] = 9
	if ok, err := i.AcceptEvent(1, []byte{1}); ok || err != nil {
		t.Fatal(ok, err)
	}
	if _, err := i.AcceptEvent(2, []byte{9}); !errors.Is(err, ErrMismatch) {
		t.Fatal(err)
	}
	if err := i.Complete(2); err != nil {
		t.Fatal(err)
	}
}

func TestEventConcurrentAndCapacity(t *testing.T) {
	i, err := Open(filepath.Join(t.TempDir(), "log"), "s")
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	if _, err := i.AcceptEvent(1, bytes.Repeat([]byte{1}, storage.MaxRecordSize)); !errors.Is(err, storage.ErrTooLarge) {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var wins int32
	for n := 0; n < 30; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := i.AcceptEvent(1, []byte("x"))
			if err != nil {
				t.Error(err)
			}
			if ok {
				atomic.AddInt32(&wins, 1)
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatal(wins)
	}
	for n := 0; n < 30; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := i.Complete(1); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	events, err := i.Pending()
	if err != nil || len(events) != 0 {
		t.Fatal(events, err)
	}
}

func TestConcurrentDuplicate(t *testing.T) {
	i, err := Open(filepath.Join(t.TempDir(), "inbox"), "sender")
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	var wins int32
	var wg sync.WaitGroup
	for n := 0; n < 50; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := i.Accept(1)
			if err != nil {
				t.Error(err)
			}
			if ok {
				atomic.AddInt32(&wins, 1)
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("accepted %d times", wins)
	}
}

func TestCorruptAndInvalidLog(t *testing.T) {
	p := filepath.Join(t.TempDir(), "inbox")
	l, err := storage.Open(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []string{`{"sender":"sender"}`, `{"id":2}`} {
		if err := l.Append([]byte(b)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(p, "sender"); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid sequence: %v", err)
	}
	p = filepath.Join(t.TempDir(), "broken")
	if err := os.WriteFile(p, []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(p, "sender"); !errors.Is(err, storage.ErrCorrupt) {
		t.Fatalf("corrupt: %v", err)
	}
}

func TestQueueIdentityAndRecovery(t *testing.T) {
	const generation = "0123456789abcdef0123456789abcdef"
	const other = "1123456789abcdef0123456789abcdef"
	p := filepath.Join(t.TempDir(), "queue")
	for _, invalid := range []string{"", "0123456789ABCDEF0123456789ABCDEF", "xyz"} {
		if _, err := OpenQueue(p, "sender", invalid); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid incarnation %q: %v", invalid, err)
		}
	}
	i, err := OpenQueue(p, "sender", generation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := i.Accept(1); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	input := []byte("first")
	if ok, err := i.AcceptEvent(1, input); !ok || err != nil {
		t.Fatal(ok, err)
	}
	input[0] = 'X'
	if ok, err := i.AcceptEvent(2, []byte("second")); !ok || err != nil {
		t.Fatal(ok, err)
	}
	if err := i.Complete(1); err != nil {
		t.Fatal(err)
	}
	if err := i.Close(); err != nil {
		t.Fatal(err)
	}
	for _, identity := range [][2]string{{"other", generation}, {"sender", other}} {
		if _, err := OpenQueue(p, identity[0], identity[1]); !errors.Is(err, ErrInvalid) {
			t.Fatal(identity, err)
		}
	}
	if _, err := Open(p, "sender"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	i, err = OpenQueue(p, "sender", generation)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	for _, tc := range []struct {
		id       uint64
		payload  string
		mismatch bool
	}{
		{1, "first", false}, {1, "conflict", true}, {2, "second", false}, {2, "conflict", true},
	} {
		ok, err := i.AcceptEvent(tc.id, []byte(tc.payload))
		if ok || (tc.mismatch && !errors.Is(err, ErrMismatch)) || (!tc.mismatch && err != nil) {
			t.Fatal(tc, ok, err)
		}
	}
	if _, err := i.AcceptEvent(4, nil); !errors.Is(err, ErrGap) {
		t.Fatal(err)
	}
	items, err := i.Pending()
	if err != nil || len(items) != 1 || items[0].ID != 2 || string(items[0].Payload) != "second" {
		t.Fatal(items, err)
	}
	if err := i.Complete(2); err != nil {
		t.Fatal(err)
	}
}

func TestQueueDoesNotUpgradeLegacyLog(t *testing.T) {
	p := filepath.Join(t.TempDir(), "legacy")
	i, err := Open(p, "sender")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := i.Accept(1); err != nil {
		t.Fatal(err)
	}
	if err := i.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenQueue(p, "sender", "0123456789abcdef0123456789abcdef"); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}

// A crash after an external effect but before Complete leaves the event pending.
func TestQueueEffectBeforeCompleteIsRetried(t *testing.T) {
	p := filepath.Join(t.TempDir(), "queue")
	const generation = "0123456789abcdef0123456789abcdef"
	i, err := OpenQueue(p, "sender", generation)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := i.AcceptEvent(1, []byte("effect")); err != nil {
		t.Fatal(err)
	}
	items, err := i.Pending()
	if err != nil || len(items) != 1 {
		t.Fatal(items, err)
	}
	// Simulate a successful external effect followed by process termination.
	if err := i.Close(); err != nil {
		t.Fatal(err)
	}
	i, err = OpenQueue(p, "sender", generation)
	if err != nil {
		t.Fatal(err)
	}
	defer i.Close()
	items, err = i.Pending()
	if err != nil || len(items) != 1 || items[0].ID != 1 {
		t.Fatal(items, err)
	}
}
