package broker

import (
	"errors"
	"net"
	"testing"

	"github.com/fhmq/hmq/v4/broker/packets"
)

func TestPublishDeliveryErrors(t *testing.T) {
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName = "test/topic"
	for _, qos := range []byte{0, 1, 2} {
		c := &client{status: Disconnected, inflight: make(map[uint16]*inflightElem)}
		if err := publish(&subscription{client: c}, p, qos); !errors.Is(err, net.ErrClosed) {
			t.Fatalf("qos %d disconnected: %v", qos, err)
		}
	}
	c := &client{status: Connected, inflight: make(map[uint16]*inflightElem)}
	for id := 1; id <= 65535; id++ {
		c.inflight[uint16(id)] = &inflightElem{}
	}
	if err := publish(&subscription{client: c}, p, 1); !errors.Is(err, ErrPacketIDExhausted) {
		t.Fatalf("exhausted: %v", err)
	}
	if err := publish(&subscription{client: c}, p, 3); !errors.Is(err, ErrInvalidDeliveryQoS) {
		t.Fatalf("invalid qos: %v", err)
	}
}

func TestPublishDeliveryWriteFailureKeepsInflight(t *testing.T) {
	for _, qos := range []byte{0, 1, 2} {
		a, b := net.Pipe()
		b.Close()
		c := &client{status: Connected, conn: a, inflight: make(map[uint16]*inflightElem)}
		p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		p.TopicName = "test/topic"
		if err := publish(&subscription{client: c}, p, qos); err == nil {
			t.Fatalf("qos %d: expected write failure", qos)
		}
		want := 0
		if qos > 0 {
			want = 1
		}
		if len(c.inflight) != want {
			t.Fatalf("qos %d: got %d inflight, want %d", qos, len(c.inflight), want)
		}
		c.Close()
	}
}

func TestPublishDeliverySuccess(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := &client{status: Connected, conn: a, inflight: make(map[uint16]*inflightElem)}
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName, p.Payload = "test/topic", []byte("hello")
	result := make(chan error, 1)
	go func() { result <- publishDelivery(&subscription{client: c}, p, 1, true) }()
	packet, err := packets.ReadPacket(b)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := packet.(*packets.PublishPacket)
	if !ok || got.MessageID == 0 || !got.Retain || string(got.Payload) != "hello" {
		t.Fatalf("unexpected packet: %#v", packet)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	c.Close()
}
