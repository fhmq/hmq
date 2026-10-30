package broker

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fhmq/hmq/v3/broker/reliable"
	"github.com/fhmq/hmq/v3/broker/reliable/inbox"
)

type runtimeMessage struct {
	peer string
	data []byte
}

func TestReliableBoundLogIdentity(t *testing.T) {
	dir := t.TempDir()
	e, i := filepath.Join(dir, "e"), filepath.Join(dir, "i")
	s := runtimeSender{make(chan runtimeMessage, 100)}
	open := func(identity string) (*reliableRuntime, error) {
		return newBoundReliableRuntime("a", []string{"a", "b"}, e, i, s, time.Second, 0, func(inbox.Message) error { return nil }, []byte(identity))
	}
	r, err := open("cluster-a")
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	if r, err = open("cluster-b"); err == nil {
		r.Close()
		t.Fatal("changed identity accepted")
	}
	r, err = open("cluster-a")
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	if err = os.Remove(i); err != nil {
		t.Fatal(err)
	}
	if r, err = open("cluster-a"); err == nil {
		r.Close()
		t.Fatal("missing inbox accepted")
	}
}

type runtimeSender struct{ messages chan runtimeMessage }

func (s runtimeSender) Send(peer string, data []byte) error {
	s.messages <- runtimeMessage{peer, append([]byte(nil), data...)}
	return nil
}

func TestReliableRuntimeRecoveryAndDispatch(t *testing.T) {
	dir := t.TempDir()
	events, incoming := filepath.Join(dir, "events"), filepath.Join(dir, "inbox")
	s := runtimeSender{make(chan runtimeMessage, 100)}
	delivered := make(chan inbox.Message, 10)
	open := func() *reliableRuntime {
		r, err := newReliableRuntime("a", []string{"a", "b"}, events, incoming, s, 20*time.Millisecond, 0, func(m inbox.Message) error { delivered <- m; return nil })
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	r := open()
	if handled, err := r.handle("b", []byte("other")); handled || err != nil {
		t.Fatalf("unexpected dispatch: %v %v", handled, err)
	}
	if handled, err := r.handle("unknown", append(append([]byte{}, reliablePrefix...), []byte("{}")...)); !handled || !errors.Is(err, reliable.ErrPeer) {
		t.Fatalf("unknown peer: %v %v", handled, err)
	}
	if err := r.enqueue([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	var data runtimeMessage
	select {
	case data = <-s.messages:
	case <-time.After(time.Second):
		t.Fatal("no send")
	}
	if data.peer != "b" || !bytes.HasPrefix(data.data, reliablePrefix) {
		t.Fatalf("invalid frame: %+v", data)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.enqueue(nil); !errors.Is(err, reliable.ErrClosed) {
		t.Fatalf("enqueue after close: %v", err)
	}
	r = open()
	defer r.Close()
	select {
	case <-s.messages:
	case <-time.After(time.Second):
		t.Fatal("pending event not recovered")
	}
	if handled, err := r.handle("b", data.data); !handled || err != nil {
		t.Fatalf("data: %v %v", handled, err)
	}
	select {
	case m := <-delivered:
		if string(m.Payload) != "payload" {
			t.Fatalf("payload: %q", m.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("not delivered")
	}
}

func TestReliableRuntimeRejectsInvalidMembership(t *testing.T) {
	dir := t.TempDir()
	_, err := newReliableRuntime("a", []string{"a", "b", "b"}, filepath.Join(dir, "e"), filepath.Join(dir, "i"), runtimeSender{make(chan runtimeMessage, 1)}, time.Second, 0, func(inbox.Message) error { return nil })
	if err == nil {
		t.Fatal("duplicate member accepted")
	}
}
