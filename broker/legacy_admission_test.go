package broker

import (
	"bytes"
	"io"
	"testing"

	"github.com/fhmq/hmq/v4/broker/packets"
)

func TestLegacyPacketLimitBeforeBodyRead(t *testing.T) {
	r := &mqtt5ValidationHeaderReader{header: bytes.NewReader(append(
		[]byte{packets.Publish << 4}, mqtt5ValidationVBI(mqtt5MaximumPacketSize)...,
	))}
	c := &client{}
	if p, _, err := c.readClientPacket(r); p != nil || mqtt5Reason(err) != 0x95 || r.bodyReads != 0 {
		t.Fatalf("packet=%v, error=%v, body reads=%d", p, err, r.bodyReads)
	}
}

type stalledFrameReader struct {
	header *bytes.Reader
	first  int
}

func (r *stalledFrameReader) Read(p []byte) (int, error) {
	if r.header.Len() != 0 {
		return r.header.Read(p)
	}
	r.first = len(p)
	return 0, io.EOF
}

func TestIncompleteFrameDoesNotPreallocateBody(t *testing.T) {
	r := &stalledFrameReader{header: bytes.NewReader(append(
		[]byte{packets.Publish << 4}, mqtt5ValidationVBI(mqtt5MaximumPacketSize/2)...,
	))}
	if _, err := readMQTTFrame(r, mqtt5MaximumPacketSize); err != io.ErrUnexpectedEOF || r.first == 0 || r.first > 4096 {
		t.Fatalf("first body read requested %d bytes; err=%v", r.first, err)
	}
}

func TestDetachedClientMalformedPacket(t *testing.T) {
	c := &client{status: Connected}
	p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
	p.TopicName = "bad\x00topic"
	ProcessMessage(&Message{client: c, packet: p})
	if !c.isDisconnected() {
		t.Fatal("malformed packet did not terminate connection state")
	}
	ProcessMessage(&Message{client: c, packet: p})
}

func TestLegacyPublishAcceptsBinaryAndEmptyPayload(t *testing.T) {
	for _, payload := range [][]byte{nil, {0, 255}} {
		p := packets.NewControlPacket(packets.Publish).(*packets.PublishPacket)
		p.TopicName, p.Payload, p.Qos, p.MessageID = "data", payload, 2, 1
		var encoded bytes.Buffer
		if err := p.Write(&encoded); err != nil {
			t.Fatal(err)
		}
		got, _, err := (&client{}).readClientPacket(&encoded)
		decoded, ok := got.(*packets.PublishPacket)
		if err != nil || !ok || decoded.MessageID != 1 || decoded.Qos != 2 || !bytes.Equal(decoded.Payload, payload) {
			t.Fatalf("PUBLISH=%#v, err=%v", got, err)
		}
	}
}
