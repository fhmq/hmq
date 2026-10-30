package reliable

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fhmq/hmq/v3/broker/eventstore"
)

type testSender struct {
	mu     sync.Mutex
	frames [][]byte
	fail   bool
}

func (s *testSender) Send(_ string, b []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("disconnected")
	}
	s.frames = append(s.frames, append([]byte(nil), b...))
	return nil
}
func (s *testSender) count() int     { s.mu.Lock(); defer s.mu.Unlock(); return len(s.frames) }
func (s *testSender) setFail(v bool) { s.mu.Lock(); defer s.mu.Unlock(); s.fail = v }
func wait(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(time.Millisecond)
	}
}
func wire(typ, id string) []byte {
	b, _ := json.Marshal(frame{Version: 1, Type: typ, ID: id})
	return b
}
func open(t *testing.T, path string) *eventstore.Store {
	t.Helper()
	s, err := eventstore.Open(path, eventstore.Limits{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestDisconnectAckAndRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events")
	s := open(t, path)
	send := &testSender{fail: true}
	l, err := New("peer", send, s, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Enqueue("one", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if send.count() != 0 || !l.Pending("one") {
		t.Fatal("disconnected delivery lost")
	}
	if err := l.Handle("other", wire("ack", "one"), nil); !errors.Is(err, ErrPeer) {
		t.Fatal(err)
	}
	if err := l.Handle("peer", wire("ack", "unknown"), nil); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
	send.setFail(false)
	wait(t, func() bool { return send.count() >= 2 })
	l.Close()
	s.Close()
	s = open(t, path)
	defer s.Close()
	before := send.count()
	l, err = New("peer", send, s, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if !l.Pending("one") {
		t.Fatal("restart did not recover pending event")
	}
	wait(t, func() bool { return send.count() > before })
	if err := l.Handle("peer", wire("ack", "one"), nil); err != nil {
		t.Fatal(err)
	}
	e, err := s.Get("one")
	if err != nil || len(e.Confirmed) != 1 || l.Pending("one") {
		t.Fatalf("event=%+v err=%v", e, err)
	}
	if err := l.Resume("one"); err != nil || l.Pending("one") {
		t.Fatalf("resume confirmed: %v", err)
	}
}

type failStore struct {
	*eventstore.Store
	putErr, ackErr error
}

func (s *failStore) Put(id string, p []byte, targets []string) error {
	if s.putErr != nil {
		return s.putErr
	}
	return s.Store.Put(id, p, targets)
}
func (s *failStore) Ack(id, target string) error {
	if s.ackErr != nil {
		return s.ackErr
	}
	return s.Store.Ack(id, target)
}
func TestStoreFailures(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "events"))
	defer s.Close()
	boom := errors.New("storage failure")
	f := &failStore{Store: s, putErr: boom}
	send := &testSender{}
	l, _ := New("peer", send, f, time.Hour)
	defer l.Close()
	if !errors.Is(l.Enqueue("a", nil), boom) || l.Pending("a") {
		t.Fatal("failed put scheduled")
	}
	f.putErr = nil
	if err := l.Enqueue("a", nil); err != nil {
		t.Fatal(err)
	}
	wait(t, func() bool { return send.count() > 0 })
	f.ackErr = boom
	if !errors.Is(l.Handle("peer", wire("ack", "a"), nil), boom) || !l.Pending("a") {
		t.Fatal("failed ack removed pending")
	}
	f.ackErr = nil
	if err := l.Handle("peer", wire("ack", "a"), nil); err != nil {
		t.Fatal(err)
	}
}

func TestReceiverAndClose(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "events"))
	defer s.Close()
	send := &testSender{}
	l, _ := New("peer", send, s, time.Hour)
	boom := errors.New("application failure")
	if err := l.Handle("peer", wire("data", "x"), func(string, []byte) error { return boom }); !errors.Is(err, boom) || send.count() != 0 {
		t.Fatal("ack on failed delivery")
	}
	send.setFail(true)
	if err := l.Handle("peer", wire("data", "x"), func(string, []byte) error { return nil }); err == nil {
		t.Fatal("expected ack failure")
	}
	send.setFail(false)
	if err := l.Handle("peer", wire("data", "x"), func(string, []byte) error { return nil }); err != nil || send.count() != 1 {
		t.Fatal(err)
	}
	l.Close()
	l.Close()
	if !errors.Is(l.Enqueue("x", nil), ErrClosed) {
		t.Fatal("enqueue after close")
	}
	if !errors.Is(l.Handle("peer", wire("data", "x"), nil), ErrClosed) {
		t.Fatal("handle after close")
	}
}

func TestRejectInvalidUTF8AckIdentity(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "events"))
	defer s.Close()
	l, err := New("peer", &testSender{}, s, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	id := "\ufffd"
	if err := l.Enqueue(id, nil); err != nil {
		t.Fatal(err)
	}
	bad := append([]byte(`{"version":1,"type":"ack","id":"`), 0xff)
	bad = append(bad, []byte(`"}`)...)
	if err := l.Handle("peer", bad, nil); !errors.Is(err, ErrFrame) {
		t.Fatalf("invalid UTF-8 ACK: %v", err)
	}
	e, err := s.Get(id)
	if err != nil || len(e.Confirmed) != 0 || !l.Pending(id) {
		t.Fatalf("invalid ACK changed event: %+v, %v", e, err)
	}
}

func TestUnicodeSurrogateFrames(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "events"))
	defer s.Close()
	send := &testSender{fail: true}
	l, err := New("peer", send, s, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Enqueue("\ufffd", nil); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{`\ud800`, `\udfff`, `\ud800\u0041`, `\ud800\ud800`, `\ud800x`} {
		for _, typ := range []string{"ack", "data"} {
			t.Run(typ+id, func(t *testing.T) {
				called := false
				data := []byte(`{"version":1,"type":"` + typ + `","id":"` + id + `"}`)
				if err := l.Handle("peer", data, func(string, []byte) error { called = true; return nil }); !errors.Is(err, ErrFrame) || called {
					t.Fatalf("Handle = %v, delivered = %v", err, called)
				}
			})
		}
	}
	e, err := s.Get("\ufffd")
	if err != nil || len(e.Confirmed) != 0 || !l.Pending("\ufffd") {
		t.Fatalf("invalid ACK changed event: %+v, %v", e, err)
	}
	if err := l.Enqueue("\U0001f600", nil); err != nil {
		t.Fatal(err)
	}
	if err := l.Handle("peer", []byte(`{"version":1,"type":"ack","id":"\ud83d\ude00"}`), nil); err != nil {
		t.Fatalf("valid surrogate pair: %v", err)
	}
	if l.Pending("\U0001f600") {
		t.Fatal("valid ACK did not confirm event")
	}
	if err := l.Enqueue(`\ud800`, nil); err != nil {
		t.Fatal(err)
	}
	if err := l.Handle("peer", []byte(`{"version":1,"type":"ack","id":"\\ud800"}`), nil); err != nil {
		t.Fatalf("escaped backslash: %v", err)
	}
}

func TestRecoveryLoadsNextBatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events")
	s := open(t, path)
	for i := 0; i <= recoveryBatch; i++ {
		if err := s.Put(fmt.Sprintf("id-%03d", i), nil, []string{"peer"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path)
	defer s.Close()
	send := &testSender{fail: true}
	l, err := New("peer", send, s, 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	last := fmt.Sprintf("id-%03d", recoveryBatch)
	if l.Pending(last) {
		t.Fatal("loaded beyond recovery batch")
	}
	if err := l.Handle("peer", wire("ack", "id-000"), nil); err != nil {
		t.Fatal(err)
	}
	wait(t, func() bool { return l.Pending(last) })
}

func TestLateAndDuplicateAckAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events")
	s := open(t, path)
	for i := 0; i <= recoveryBatch; i++ {
		if err := s.Put(fmt.Sprintf("id-%03d", i), nil, []string{"peer"}); err != nil {
			t.Fatal(err)
		}
	}
	l, err := New("peer", &testSender{fail: true}, s, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	last := fmt.Sprintf("id-%03d", recoveryBatch)
	if l.Pending(last) {
		t.Fatal("event unexpectedly in initial batch")
	}
	if err := l.Handle("peer", wire("ack", last), nil); err != nil {
		t.Fatalf("late ACK outside batch: %v", err)
	}
	if err := l.Handle("peer", wire("ack", last), nil); err != nil {
		t.Fatalf("duplicate ACK: %v", err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path)
	defer s.Close()
	l, err = New("peer", &testSender{fail: true}, s, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Handle("peer", wire("ack", last), nil); err != nil {
		t.Fatalf("duplicate after restart: %v", err)
	}
	e, err := s.Get(last)
	if err != nil || len(e.Confirmed) != 1 || e.Confirmed[0] != "peer" {
		t.Fatalf("confirmation not recovered: %+v %v", e, err)
	}
	if err := l.Handle("peer", wire("ack", "missing"), nil); !errors.Is(err, ErrUnknown) {
		t.Fatalf("unknown ACK: %v", err)
	}
}

func TestPeerIsolationAndConcurrentClose(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events")
	s := open(t, path)
	a, err := New("a", &testSender{}, s, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New("b", &testSender{}, s, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Enqueue("shared", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := b.Enqueue("shared", []byte("payload")); !errors.Is(err, eventstore.ErrConflict) {
		t.Fatalf("collision: %v", err)
	}
	if err := a.Handle("b", wire("ack", "shared"), nil); !errors.Is(err, ErrPeer) {
		t.Fatalf("forged peer: %v", err)
	}
	if err := b.Handle("b", wire("ack", "shared"), nil); !errors.Is(err, ErrUnknown) {
		t.Fatalf("foreign ID: %v", err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); a.Close() }()
	}
	wg.Wait()
	b.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = open(t, path)
	defer s.Close()
	e, err := s.Get("shared")
	if err != nil || len(e.Confirmed) != 0 {
		t.Fatalf("event=%+v err=%v", e, err)
	}
	l, err := New("a", &testSender{}, s, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if !l.Pending("shared") {
		t.Fatal("pending event lost after restart")
	}
}

type testInbox struct{ store *eventstore.Store }

func (i testInbox) Accept(peer, id string, payload []byte) error {
	key, _ := json.Marshal([]string{peer, id})
	err := i.store.Put(string(key), payload, []string{"consumer"})
	if errors.Is(err, eventstore.ErrConflict) {
		return ErrConflict
	}
	return err
}

func TestInboxRecoveryAndConflicts(t *testing.T) {
	dir := t.TempDir()
	out := open(t, filepath.Join(dir, "out"))
	defer out.Close()
	path := filepath.Join(dir, "in")
	in := open(t, path)
	send := &testSender{fail: true}
	l, err := New("peer", send, out, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	data, _ := json.Marshal(frame{Version: 1, Type: "data", ID: "incarnation-1/message-1", Payload: []byte("value")})
	if err := l.HandleInbox("peer", data, testInbox{in}); err == nil {
		t.Fatal("expected lost ACK")
	}
	if err := in.Close(); err != nil {
		t.Fatal(err)
	}
	in = open(t, path)
	defer in.Close()
	send.setFail(false)
	var wg sync.WaitGroup
	errs := make(chan error, 20)
	for n := 0; n < 20; n++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- l.HandleInbox("peer", data, testInbox{in}) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if send.count() != 20 {
		t.Fatalf("ACKs: %d", send.count())
	}
	bad, _ := json.Marshal(frame{Version: 1, Type: "data", ID: "incarnation-1/message-1", Payload: []byte("different")})
	if err := l.HandleInbox("peer", bad, testInbox{in}); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict: %v", err)
	}
	if send.count() != 20 {
		t.Fatal("ACK sent for conflict")
	}
	key, _ := json.Marshal([]string{"peer", "incarnation-1/message-1"})
	e, err := in.Get(string(key))
	if err != nil || string(e.Payload) != "value" {
		t.Fatalf("recovered event: %+v, %v", e, err)
	}
}

func TestEnqueueFrameSize(t *testing.T) {
	for _, tc := range []struct {
		name, id string
		payload  []byte
	}{
		{"utf8 and escapes", "消息\"\\\n", []byte("abc")},
		{"base64 padding", "binary", []byte{0, 255, 1, 2}},
		{"empty", "empty", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			encoded, err := json.Marshal(frame{Version: 1, Type: "data", ID: tc.id, Payload: tc.payload})
			if err != nil {
				t.Fatal(err)
			}
			for _, delta := range []int{-1, 0, 1} {
				s := open(t, filepath.Join(t.TempDir(), "events"))
				l, err := NewWithMaxFrameSize("peer", &testSender{}, s, time.Hour, uint32(len(encoded)+delta))
				if err != nil {
					t.Fatal(err)
				}
				err = l.Enqueue(tc.id, tc.payload)
				if delta < 0 {
					if !errors.Is(err, ErrFrameTooLarge) {
						t.Fatalf("oversize: %v", err)
					}
					if _, err := s.Get(tc.id); err == nil || l.Pending(tc.id) {
						t.Fatal("oversized event persisted or scheduled")
					}
				} else if err != nil {
					t.Fatalf("within limit: %v", err)
				}
				l.Close()
				s.Close()
			}
		})
	}
	s := open(t, filepath.Join(t.TempDir(), "events"))
	defer s.Close()
	l, err := New("peer", &testSender{}, s, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for _, id := range []string{"", string([]byte{0xff})} {
		if err := l.Enqueue(id, nil); !errors.Is(err, ErrFrame) {
			t.Fatalf("invalid ID: %v", err)
		}
	}
}

func TestOversizedRecovery(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "events"))
	defer s.Close()
	if err := s.Put("large", make([]byte, 100), []string{"peer"}); err != nil {
		t.Fatal(err)
	}
	if _, err := NewWithMaxFrameSize("peer", &testSender{}, s, time.Hour, 100); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("startup: %v", err)
	}
}

func TestOversizedLaterBatchAndResume(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "events"))
	defer s.Close()
	for i := 0; i < recoveryBatch; i++ {
		if err := s.Put(fmt.Sprintf("id-%03d", i), nil, []string{"peer"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put("zz-large", make([]byte, 100), []string{"peer"}); err != nil {
		t.Fatal(err)
	}
	send := &testSender{fail: true}
	l, err := NewWithMaxFrameSize("peer", send, s, time.Millisecond, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Resume("zz-large"); !errors.Is(err, ErrFrameTooLarge) || l.Pending("zz-large") {
		t.Fatalf("resume: %v", err)
	}
	if err := l.Handle("peer", wire("ack", "id-000"), nil); err != nil {
		t.Fatal(err)
	}
	wait(t, func() bool { return l.Err() != nil })
	if !errors.Is(l.Err(), ErrFrameTooLarge) || l.Pending("zz-large") {
		t.Fatalf("background: %v", l.Err())
	}
	if send.count() != 0 {
		t.Fatal("unexpected successful send")
	}
}

func TestMultiTargetLaterBatchReportsError(t *testing.T) {
	s := open(t, filepath.Join(t.TempDir(), "events"))
	defer s.Close()
	for i := 0; i < recoveryBatch; i++ {
		if err := s.Put(fmt.Sprintf("id-%03d", i), nil, []string{"peer"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Put("multi", nil, []string{"peer", "other"}); err != nil {
		t.Fatal(err)
	}
	l, err := New("peer", &testSender{fail: true}, s, time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	if err := l.Handle("peer", wire("ack", "id-000"), nil); err != nil {
		t.Fatal(err)
	}
	wait(t, func() bool { return l.Err() != nil })
	if !errors.Is(l.Err(), ErrPeer) || l.Pending("multi") {
		t.Fatalf("background: %v", l.Err())
	}
}
