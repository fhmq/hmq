package broker

import (
	"fmt"
	"github.com/eclipse/paho.mqtt.golang/packets"
	"github.com/fhmq/hmq/v3/broker/sessionstore"
	"path/filepath"
	"testing"
)

func TestDurableWindowBounds(t *testing.T) {
	for _, tc := range []struct{ requested, want int }{{0, 128}, {-1, 128}, {2, 2}, {65535, 65535}, {65536, 65535}} {
		b := &Broker{sessionWindow: tc.requested}
		if got := b.durableWindow(); got != tc.want {
			t.Fatalf("window(%d)=%d, want %d", tc.requested, got, tc.want)
		}
	}
}

func TestSessionLogWindowDrainsFIFO(t *testing.T) {
	for _, qos := range []byte{1, 2} {
		t.Run(fmt.Sprint(qos), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "sessions.log")
			open := func() *Broker {
				b, err := NewBroker(&Config{ExperimentalSessionLogPath: path})
				if err != nil {
					t.Fatal(err)
				}
				b.sessionWindow = 2
				t.Cleanup(func() { b.Close() })
				return b
			}
			b := open()
			peer, done := logMQTTConnect(t, b, "window", false)
			sub := packets.NewControlPacket(packets.Subscribe).(*packets.SubscribePacket)
			sub.MessageID, sub.Topics, sub.Qoss = 1, []string{"window/topic"}, []byte{qos}
			if err := sub.Write(peer); err != nil {
				t.Fatal(err)
			}
			if _, err := packets.ReadPacket(peer); err != nil {
				t.Fatal(err)
			}
			peer.Close()
			waitLogDone(t, done)
			var backlog []sessionstore.Message
			for n := 0; n < 7; n++ {
				backlog = append(backlog, sessionstore.Message{Topic: "window/topic", Payload: []byte(fmt.Sprint(n)), QoS: qos})
			}
			if err := b.applySession("window", sessionstore.Batch{Enqueue: backlog}); err != nil {
				t.Fatal(err)
			}
			b.Close()
			b = open()
			peer, done = logMQTTConnect(t, b, "window", true)
			read := func(want int, dup bool) *packets.PublishPacket {
				t.Helper()
				a, err := packets.ReadPacket(peer)
				if err != nil {
					t.Fatal(err)
				}
				p, ok := a.(*packets.PublishPacket)
				if !ok || string(p.Payload) != fmt.Sprint(want) || p.Dup != dup || p.Qos != qos {
					t.Fatalf("delivery %d: %#v", want, a)
				}
				return p
			}
			first, second := read(0, true), read(1, true)
			state, _, err := b.sessionLog.Get("window")
			if err != nil || len(state.Inflight) != 2 || len(state.Queue) != 5 {
				t.Fatalf("unbounded recovery: %#v %v", state, err)
			}
			// A new online publication must wait behind the entire offline backlog.
			p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
			p.TopicName, p.Payload, p.Qos = "window/topic", []byte("7"), qos
			if err := b.publishLocal(p); err != nil {
				t.Fatal(err)
			}
			ack := func(p *packets.PublishPacket) {
				t.Helper()
				if qos == 1 {
					a := packets.NewControlPacket(packets.Puback).(*packets.PubackPacket)
					a.MessageID = p.MessageID
					if err := a.Write(peer); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := packets.NewControlPacket(packets.Pingreq).Write(peer); err != nil {
						t.Fatal(err)
					}
					if _, err := packets.ReadPacket(peer); err != nil {
						t.Fatal(err)
					}
					before, _, _ := b.sessionLog.Get("window")
					a := packets.NewControlPacket(packets.Pubrec).(*packets.PubrecPacket)
					a.MessageID = p.MessageID
					if err := a.Write(peer); err != nil {
						t.Fatal(err)
					}
					r, err := packets.ReadPacket(peer)
					if err != nil {
						t.Fatal(err)
					}
					if rel, ok := r.(*packets.PubrelPacket); !ok || rel.MessageID != p.MessageID {
						t.Fatalf("PUBREL: %#v", r)
					}
					after, _, _ := b.sessionLog.Get("window")
					if len(after.Queue) != len(before.Queue) || len(after.Inflight) != len(before.Inflight) {
						t.Fatal("PUBREC released window slot")
					}
					comp := packets.NewControlPacket(packets.Pubcomp).(*packets.PubcompPacket)
					comp.MessageID = p.MessageID
					if err := comp.Write(peer); err != nil {
						t.Fatal(err)
					}
				}
			}
			ack(first)
			third := read(2, false)
			state, _, _ = b.sessionLog.Get("window")
			if len(state.Inflight) != 2 || len(state.Queue) != 5 {
				t.Fatalf("ACK failed to refill: %#v", state)
			}
			// Restart with both inflight and a larger queued suffix; preserve order/IDs.
			peer.Close()
			waitLogDone(t, done)
			b.Close()
			b = open()
			peer, done = logMQTTConnect(t, b, "window", true)
			replaySecond, replayThird := read(1, true), read(2, true)
			if replaySecond.MessageID != second.MessageID || replayThird.MessageID != third.MessageID {
				t.Fatal("restart changed packet IDs")
			}
			pending := []*packets.PublishPacket{replaySecond, replayThird}
			for n := 3; n < 8; n++ {
				ack(pending[0])
				pending = pending[1:]
				pending = append(pending, read(n, false))
			}
			// No refill remains, so ACK the final two and fence their durable commits.
			for _, p := range pending {
				ack(p)
			}
			if err := packets.NewControlPacket(packets.Pingreq).Write(peer); err != nil {
				t.Fatal(err)
			}
			if _, err := packets.ReadPacket(peer); err != nil {
				t.Fatal(err)
			}
			state, _, err = b.sessionLog.Get("window")
			if err != nil || len(state.Queue) != 0 || len(state.Inflight) != 0 {
				t.Fatalf("backlog not drained: %#v %v", state, err)
			}
			peer.Close()
			waitLogDone(t, done)
		})
	}
}
