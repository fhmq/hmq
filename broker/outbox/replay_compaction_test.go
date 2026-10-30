package outbox

import (
	"testing"
)

func TestReplayLongAcknowledgedHistory(t *testing.T) {
	l := &failingLog{}
	o, err := recoverLog(l, "peer", 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	incarnation := o.Incarnation()
	first, err := o.Enqueue([]byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2100; i++ {
		id, err := o.Enqueue([]byte("x"))
		if err != nil {
			t.Fatal(err)
		}
		if err := o.Ack(id); err != nil {
			t.Fatal(err)
		}
	}
	last, err := o.Enqueue([]byte("last"))
	if err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	o, err = recoverLog(l, "peer", 10, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if o.Incarnation() != incarnation {
		t.Fatal("incarnation changed")
	}
	pending, err := o.Pending()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 || pending[0].ID != first || string(pending[0].Payload) != "first" || pending[1].ID != last || string(pending[1].Payload) != "last" {
		t.Fatalf("unexpected pending: %+v", pending)
	}
	next, err := o.Enqueue(nil)
	if err != nil || next != last+1 {
		t.Fatalf("next ID = %d, error = %v", next, err)
	}
}
