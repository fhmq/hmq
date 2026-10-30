package broker

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/fhmq/hmq/v3/broker/sessionstore"
)

func TestSessionIndexRecoveryPagesSubscriptions(t *testing.T) {
	store, err := sessionstore.Open(filepath.Join(t.TempDir(), "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	state := &sessionstore.State{Subscriptions: make(map[string]byte), Queue: []sessionstore.Message{{Topic: "queued", QoS: 1, Payload: make([]byte, 1<<20)}}}
	for i := 0; i < 300; i++ {
		state.Subscriptions[fmt.Sprintf("topic/%03d", i)] = byte(i % 3)
	}
	if _, err := store.Put("client", 0, state); err != nil {
		t.Fatal(err)
	}
	b := &Broker{sessionLog: store}
	if err := b.initSessionIndex(); err != nil {
		t.Fatal(err)
	}
	defer b.sessionIndex.matcher.Close()
	if len(b.sessionIndex.filters["client"]) != 300 {
		t.Fatalf("incomplete index: %d", len(b.sessionIndex.filters["client"]))
	}
	for _, i := range []int{0, 127, 128, 255, 256, 299} {
		var subs []interface{}
		var qoss []byte
		if err := b.sessionIndex.matcher.Subscribers([]byte(fmt.Sprintf("topic/%03d", i)), 2, &subs, &qoss); err != nil || len(subs) != 1 || qoss[0] != byte(i%3) {
			t.Fatalf("filter %d: %v %v %v", i, subs, qoss, err)
		}
	}
}
