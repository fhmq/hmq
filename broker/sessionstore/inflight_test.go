package sessionstore

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/fhmq/hmq/v3/broker/storage"
)

func TestInflightRecoveryAndAck(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.Put("c", 0, &State{Queue: []Message{{Topic: "a", QoS: 1, Payload: []byte("one")}, {Topic: "b", QoS: 2, Payload: []byte("two")}}})
	if err != nil {
		t.Fatal(err)
	}
	v, err = s.Apply("c", v, Batch{Move: []uint16{7, 9}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Apply("c", v, Batch{Acks: []Ack{{9, "PUBCOMP"}}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	v, err = s.Apply("c", v, Batch{Acks: []Ack{{7, "PUBACK"}, {9, "PUBREC"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	items, version, err := s.ReadInflight("c", 0, 1, 3)
	if err != nil || version != v || len(items) != 1 || items[0].PacketID != 9 || items[0].Inflight.Phase != PubRel {
		t.Fatalf("%+v %d %v", items, version, err)
	}
	items[0].Inflight.Message.Payload[0] = 'x'
	got, _, _ := s.Get("c")
	if string(got.Inflight[9].Message.Payload) != "two" || len(got.Queue) != 0 {
		t.Fatal(got)
	}
	if _, err = s.Apply("c", v-1, Batch{Acks: []Ack{{9, "PUBCOMP"}}}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if _, err = s.Apply("c", v, Batch{Acks: []Ack{{9, "PUBCOMP"}}}); err != nil {
		t.Fatal(err)
	}
}

func TestBoundedReadsAndConcurrentMove(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "log"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	v, err := s.Put("c", 0, &State{Queue: []Message{{Topic: "a", QoS: 1, Payload: []byte("abc")}, {Topic: "b", QoS: 2, Payload: []byte("def")}}})
	if err != nil {
		t.Fatal(err)
	}
	page, version, err := s.ReadQueue("c", 0, 2, 4)
	if err != nil || version != v || len(page) != 1 {
		t.Fatalf("%v %d %v", page, version, err)
	}
	page[0].Payload[0] = 'x'
	if _, _, err = s.ReadQueue("c", 0, 1, 2); !errors.Is(err, storage.ErrTooLarge) {
		t.Fatal(err)
	}
	if _, err = s.Apply("c", v, Batch{Move: []uint16{1, 1}}); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	const n = 16
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := s.Apply("c", v, Batch{Move: []uint16{1}}); errs <- err }()
	}
	wg.Wait()
	close(errs)
	wins := 0
	for err := range errs {
		if err == nil {
			wins++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	if wins != 1 {
		t.Fatal(wins)
	}
	got, _, _ := s.Get("c")
	if len(got.Queue) != 1 || string(got.Inflight[1].Message.Payload) != "abc" {
		t.Fatal(got)
	}
}

func TestAppendFailurePoisonsStore(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.Put("c", 0, &State{Queue: []Message{{Topic: "a", QoS: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.log.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Apply("c", v, Batch{Move: []uint16{1}}); err == nil {
		t.Fatal("expected append failure")
	}
	if _, _, err := s.Get("c"); err == nil {
		t.Fatal("expected poisoned store")
	}
	_ = s.Close()
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, version, err := s.Get("c")
	if err != nil || version != v || len(got.Queue) != 1 || len(got.Inflight) != 0 {
		t.Fatalf("%+v %d %v", got, version, err)
	}
}

func TestSurrogateLogValidation(t *testing.T) {
	for _, tc := range []struct {
		name, id string
		valid    bool
	}{
		{"high", `\ud800`, false}, {"low", `\udc00`, false},
		{"wrong-pair", `\ud800\u0041`, false}, {"pair", `\ud83d\ude00`, true},
		{"replacement", `\ufffd`, true}, {"escaped-backslash", `\\ud800`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "log")
			l, err := storage.Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := l.Append([]byte(`{"format":1,"sequence":1,"client_id":"` + tc.id + `","state":{}}`)); err != nil {
				t.Fatal(err)
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			s, err := Open(path)
			if tc.valid {
				if err != nil {
					t.Fatal(err)
				}
				s.Close()
			} else if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("got %v", err)
			}
		})
	}
}

func TestSessionAndSubscriptionPages(t *testing.T) {
	s, err := Open(filepath.Join(t.TempDir(), "log"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	v, err := s.Put("a", 0, &State{Subscriptions: map[string]byte{"a/#": 1, "b/#": 2}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Put("b", 0, &State{}); err != nil {
		t.Fatal(err)
	}
	ids, seq, err := s.List("", 1, 1)
	if err != nil || len(ids) != 1 || ids[0] != "a" {
		t.Fatalf("%v %v", ids, err)
	}
	ids, seq2, err := s.List("a", 1, 1)
	if err != nil || seq2 != seq || len(ids) != 1 || ids[0] != "b" {
		t.Fatalf("%v %v", ids, err)
	}
	if _, _, err := s.List("", 1, 0); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	subs, version, err := s.ReadSubscriptions("a", "", 2, 3)
	if err != nil || version != v || len(subs) != 1 || subs[0].Filter != "a/#" || subs[0].QoS != 1 {
		t.Fatalf("%v %d %v", subs, version, err)
	}
	if _, _, err := s.ReadSubscriptions("a", "", 1, 2); !errors.Is(err, storage.ErrTooLarge) {
		t.Fatal(err)
	}
	if _, err := s.Put("a", v, nil); err != nil {
		t.Fatal(err)
	}
	ids, seq2, err = s.List("", 2, 2)
	if err != nil || seq2 == seq || len(ids) != 1 || ids[0] != "b" {
		t.Fatalf("%v %d %v", ids, seq2, err)
	}
}

func TestInflightOrderRestartAndAckBoundaries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	v, err := s.Put("c", 0, &State{Queue: []Message{{Topic: "first", QoS: 2}, {Topic: "second", QoS: 1}, {Topic: "third", QoS: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	v, err = s.Apply("c", v, Batch{Move: []uint16{65535, 1}})
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []Batch{{Acks: []Ack{{65535, "PUBREC"}, {65535, "PUBCOMP"}}}, {Acks: []Ack{{1, "PUBACK"}}, Move: []uint16{1}}} {
		if _, err := s.Apply("c", v, b); !errors.Is(err, ErrInvalid) {
			t.Fatalf("batch %+v: %v", b, err)
		}
	}
	v, err = s.Apply("c", v, Batch{Acks: []Ack{{1, "PUBACK"}}})
	if err != nil {
		t.Fatal(err)
	}
	v, err = s.Apply("c", v, Batch{Move: []uint16{1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	first, version, err := s.ReadInflight("c", 0, 1, 100)
	if err != nil || version != v || len(first) != 1 || first[0].PacketID != 65535 || first[0].Inflight.Sequence != 1 {
		t.Fatalf("%+v %d %v", first, version, err)
	}
	second, version, err := s.ReadInflight("c", first[0].Inflight.Sequence, 1, 100)
	if err != nil || version != v || len(second) != 1 || second[0].PacketID != 1 || second[0].Inflight.Sequence != 3 {
		t.Fatalf("%+v %d %v", second, version, err)
	}
}

func TestLegacyInflightRecovery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	l, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []string{
		`{"format":1,"sequence":1,"client_id":"c","state":{"queue":[{"topic":"a","qos":1},{"topic":"b","qos":1}]}}`,
		`{"format":2,"sequence":2,"client_id":"c","state":null,"batch":{"move":[10,1]}}`,
	} {
		if err := l.Append([]byte(record)); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	items, _, err := s.ReadInflight("c", 0, 2, 100)
	if err != nil || len(items) != 2 || items[0].PacketID != 10 || items[1].PacketID != 1 {
		t.Fatalf("%+v %v", items, err)
	}
}

func TestLegacyAmbiguousSnapshotFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "log")
	l, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append([]byte(`{"format":1,"sequence":1,"client_id":"c","state":{"inflight":{"1":{"message":{"topic":"a","qos":1},"phase":"publish"},"2":{"message":{"topic":"b","qos":1},"phase":"publish"}}}}`)); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
}
