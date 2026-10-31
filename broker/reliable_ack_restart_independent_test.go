package broker

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/fhmq/hmq/v4/broker/reliable/inbox"
)

func TestReliableLostAckAfterSenderRestart(t *testing.T) {
	dir := t.TempDir()
	aWire := runtimeSender{make(chan runtimeMessage, 100)}
	bWire := runtimeSender{make(chan runtimeMessage, 100)}
	delivered := make(chan inbox.Message, 10)
	open := func(id string, wire runtimeSender) *reliableRuntime {
		r, err := newBoundReliableRuntime(id, []string{"a", "b"}, filepath.Join(dir, id+"-events"), filepath.Join(dir, id+"-inbox"), wire, 25*time.Millisecond, 0, func(m inbox.Message) error { delivered <- m; return nil }, []byte(id))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	read := func(ch <-chan runtimeMessage) runtimeMessage {
		select {
		case m := <-ch:
			return m
		case <-time.After(3 * time.Second):
			t.Fatal("frame timeout")
			return runtimeMessage{}
		}
	}
	a, b := open("a", aWire), open("b", bWire)
	defer func() { a.Close(); b.Close() }()
	if err := a.enqueue([]byte("payload")); err != nil {
		t.Fatal(err)
	}
	data := read(aWire.messages)
	if _, err := b.handle("a", data.data); err != nil {
		t.Fatal(err)
	}
	_ = read(bWire.messages) // Lose the first acknowledgement.
	select {
	case <-delivered:
	case <-time.After(3 * time.Second):
		t.Fatal("delivery timeout")
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	// Discard any frames produced before shutdown.
	for len(aWire.messages) > 0 {
		<-aWire.messages
	}
	a = open("a", aWire)
	retry := read(aWire.messages)
	if _, err := b.handle("a", retry.data); err != nil {
		t.Fatal(err)
	}
	ack := read(bWire.messages)
	if _, err := a.handle("b", ack.data); err != nil {
		t.Fatal(err)
	}
	select {
	case <-delivered:
		t.Fatal("duplicate delivery after durable acceptance")
	case <-time.After(150 * time.Millisecond):
	}
}
