package sessions

import (
	"sync"
	"testing"

	"github.com/fhmq/hmq/v4/broker/packets"
)

func TestOutboundTakeoverRejectsOldOwner(t *testing.T) {
	s := &Session{}
	old, _ := s.ClaimOutbound()
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	s.UpdateOutbound(old, 1, &OutboundMessage{Packet: p})
	current, snapshot := s.ClaimOutbound()
	if len(snapshot) != 1 {
		t.Fatal("missing delivery")
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.UpdateOutbound(old, 1, nil)
			}
		}()
	}
	wg.Wait()
	if len(s.Outbound()) != 1 {
		t.Fatal("old owner removed delivery")
	}
	if !s.UpdateOutbound(current, 1, nil) || len(s.Outbound()) != 0 {
		t.Fatal("current owner could not acknowledge delivery")
	}
}

func TestOutboundSnapshot(t *testing.T) {
	s := &Session{}
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.Payload = []byte("original")
	s.SetOutbound(7, OutboundMessage{Packet: p, AwaitingPubcomp: true})
	p.Payload[0] = 'X'
	snapshot := s.Outbound()
	if got := snapshot[7]; string(got.Packet.Payload) != "original" || !got.AwaitingPubcomp {
		t.Fatalf("unexpected stored delivery: %#v", got)
	}
	snapshot[7].Packet.Payload[0] = 'Y'
	if got := string(s.Outbound()[7].Packet.Payload); got != "original" {
		t.Fatalf("snapshot mutated stored payload: %q", got)
	}
	s.RemoveOutbound(7)
	if len(s.Outbound()) != 0 {
		t.Fatal("acknowledged delivery still present")
	}
}

func TestOutboundConcurrentAccess(t *testing.T) {
	s := &Session{}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id uint16) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				s.SetOutbound(id, OutboundMessage{Packet: &packets.PublishPacket{Payload: []byte("data")}})
				_ = s.Outbound()
				s.RemoveOutbound(id)
			}
		}(uint16(i + 1))
	}
	wg.Wait()
}
