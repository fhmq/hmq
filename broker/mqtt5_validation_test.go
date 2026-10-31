package broker

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

func mqtt5ValidationVBI(n int) []byte {
	var result []byte
	for {
		b := byte(n % 128)
		n /= 128
		if n != 0 {
			b |= 0x80
		}
		result = append(result, b)
		if n == 0 {
			return result
		}
	}
}

func mqtt5ValidationFrame(header byte, body ...byte) []byte {
	frame := append([]byte{header}, mqtt5ValidationVBI(len(body))...)
	return append(frame, body...)
}

func mqtt5ValidationConnect(properties ...byte) []byte {
	body := []byte{0, 4, 'M', 'Q', 'T', 'T', 5, 2, 0, 0}
	body = append(body, mqtt5ValidationVBI(len(properties))...)
	body = append(body, properties...)
	return mqtt5ValidationFrame(0x10, append(body, 0, 1, 'c')...)
}

// A rejected length must never cause a read request for attacker-controlled body bytes.
type mqtt5ValidationHeaderReader struct {
	header    *bytes.Reader
	bodyReads int
}

func (r *mqtt5ValidationHeaderReader) Read(p []byte) (int, error) {
	if r.header.Len() != 0 {
		return r.header.Read(p)
	}
	r.bodyReads++
	return 0, errors.New("unexpected body read")
}

func TestMQTT5ReadFrameAdversarialVBI(t *testing.T) {
	for _, tc := range []struct {
		name string
		wire []byte
	}{
		{"missing header", nil},
		{"missing remaining length", []byte{0xc0}},
		{"truncated continuation", []byte{0x30, 0x80}},
		{"unterminated four bytes", []byte{0x30, 0x80, 0x80, 0x80, 0x80}},
		{"five byte encoding", []byte{0x30, 0x80, 0x80, 0x80, 0x80, 0}},
		{"noncanonical zero", []byte{0x30, 0x80, 0}},
		{"noncanonical one", []byte{0x30, 0x81, 0, 0}},
		{"noncanonical four bytes", []byte{0x30, 0x80, 0x80, 0x80, 0}},
		{"truncated body", []byte{0x30, 3, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := readMQTTFrame(bytes.NewReader(tc.wire), mqtt5MaximumPacketSize); err == nil {
				t.Fatal("accepted malformed frame")
			}
		})
	}
}

func TestMQTT5ReadFrameRejectsBeforeBodyRead(t *testing.T) {
	for _, length := range []int{127, 128, 16384, 268435455} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			header := append([]byte{0x30}, mqtt5ValidationVBI(length)...)
			r := &mqtt5ValidationHeaderReader{header: bytes.NewReader(header)}
			frame, err := readMQTTFrame(r, 64)
			if err == nil || mqtt5Reason(err) != 0x95 {
				t.Fatalf("got frame length %d, error %v; want Packet too large", len(frame), err)
			}
			if r.bodyReads != 0 || len(frame) != 0 {
				t.Fatalf("oversized frame accessed body: reads=%d returned=%d", r.bodyReads, len(frame))
			}
		})
	}
	r := &mqtt5ValidationHeaderReader{header: bytes.NewReader([]byte{0x30, 0x80, 0x80, 0x80, 0x80})}
	if _, err := readMQTTFrame(r, mqtt5MaximumPacketSize); err == nil || r.bodyReads != 0 {
		t.Fatalf("overlong VBI: error=%v body reads=%d", err, r.bodyReads)
	}
}

func TestMQTT5ReadFrameBoundariesAndIsolation(t *testing.T) {
	for _, length := range []int{0, 1, 127, 128, 16383, 16384} {
		wire := mqtt5ValidationFrame(0x30, make([]byte, length)...)
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			r := bytes.NewReader(append(append([]byte(nil), wire...), 0xc0, 0))
			got, err := readMQTTFrame(r, len(wire))
			if err != nil || !bytes.Equal(got, wire) || r.Len() != 2 {
				t.Fatalf("boundary read: error=%v bytes=%d remaining=%d", err, len(got), r.Len())
			}
			if _, err := readMQTTFrame(bytes.NewReader(wire), len(wire)-1); err == nil || mqtt5Reason(err) != 0x95 {
				t.Fatalf("total encoded size not enforced: %v", err)
			}
		})
	}
}

func TestMQTT5ValidationRejectsMalformedPackets(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame []byte
	}{
		{"empty", nil},
		{"CONNECT reserved flag", mqtt5ValidationFrame(0x10, 0, 4, 'M', 'Q', 'T', 'T', 5, 3, 0, 0, 0, 0, 1, 'c')},
		{"will bits without will", mqtt5ValidationFrame(0x10, 0, 4, 'M', 'Q', 'T', 'T', 5, 0x22, 0, 0, 0, 0, 1, 'c')},
		{"duplicate receive maximum", mqtt5ValidationConnect(0x21, 0, 1, 0x21, 0, 2)},
		{"duplicate maximum packet size", mqtt5ValidationConnect(0x27, 0, 0, 0, 1, 0x27, 0, 0, 0, 2)},
		{"duplicate empty content type", mqtt5ValidationFrame(0x30, 0, 1, 't', 6, 3, 0, 0, 3, 0, 0)},
		{"zero receive maximum", mqtt5ValidationConnect(0x21, 0, 0)},
		{"zero maximum packet size", mqtt5ValidationConnect(0x27, 0, 0, 0, 0)},
		{"invalid boolean", mqtt5ValidationConnect(0x17, 2)},
		{"CONNECT will-only property", mqtt5ValidationConnect(0x18, 0, 0, 0, 1)},
		{"unknown property", mqtt5ValidationConnect(0x7f)},
		{"property length exceeds body", mqtt5ValidationFrame(0x30, 0, 1, 't', 5, 1, 0)},
		{"property value crosses boundary", mqtt5ValidationFrame(0x30, 0, 1, 't', 1, 1, 0)},
		{"noncanonical property length", mqtt5ValidationFrame(0x30, 0, 1, 't', 0x80, 0)},
		{"overlong property length", mqtt5ValidationFrame(0x30, 0, 1, 't', 0x80, 0x80, 0x80, 0x80, 0)},
		{"client PUBLISH subscription identifier", mqtt5ValidationFrame(0x30, 0, 1, 't', 2, 0x0b, 1)},
		{"zero topic alias", mqtt5ValidationFrame(0x30, 0, 1, 't', 3, 0x23, 0, 0)},
		{"invalid topic UTF8", mqtt5ValidationFrame(0x30, 0, 1, 0xff, 0)},
		{"surrogate topic UTF8", mqtt5ValidationFrame(0x30, 0, 3, 0xed, 0xa0, 0x80, 0)},
		{"NUL topic", mqtt5ValidationFrame(0x30, 0, 1, 0, 0)},
		{"invalid user property UTF8", mqtt5ValidationConnect(0x26, 0, 1, 0xff, 0, 0)},
		{"NUL user property value", mqtt5ValidationConnect(0x26, 0, 0, 0, 1, 0)},
		{"zero QoS1 PUBLISH ID", mqtt5ValidationFrame(0x32, 0, 1, 't', 0, 0, 0)},
		{"zero SUBSCRIBE ID", mqtt5ValidationFrame(0x82, 0, 0, 0, 0, 1, 't', 0)},
		{"zero UNSUBSCRIBE ID", mqtt5ValidationFrame(0xa2, 0, 0, 0, 0, 1, 't')},
		{"empty SUBSCRIBE", mqtt5ValidationFrame(0x82, 0, 1, 0)},
		{"empty UNSUBSCRIBE", mqtt5ValidationFrame(0xa2, 0, 1, 0)},
		{"invalid filter UTF8", mqtt5ValidationFrame(0x82, 0, 1, 0, 0, 1, 0xff, 0)},
		{"invalid wildcard position", mqtt5ValidationFrame(0x82, 0, 1, 0, 0, 3, '#', '/', 't', 0)},
		{"shared No Local", mqtt5ValidationFrame(0x82, 0, 1, 0, 0, 10, '$', 's', 'h', 'a', 'r', 'e', '/', 'g', '/', 't', 4)},
		{"zero subscription identifier", mqtt5ValidationFrame(0x82, 0, 1, 2, 11, 0, 0, 1, 't', 0)},
		{"noncanonical subscription identifier", mqtt5ValidationFrame(0x82, 0, 1, 3, 11, 0x81, 0, 0, 1, 't', 0)},
		{"PINGREQ trailing data", mqtt5ValidationFrame(0xc0, 0)},
		{"server CONNACK", mqtt5ValidationFrame(0x20, 0, 0, 0)},
		{"server PINGRESP", mqtt5ValidationFrame(0xd0)},
		{"unsupported AUTH", mqtt5ValidationFrame(0xf0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateMQTT5Frame(tc.frame); err == nil {
				t.Fatalf("accepted invalid packet: %x", tc.frame)
			}
		})
	}
}

func TestMQTT5ValidationReservedFlagsAndOptions(t *testing.T) {
	for _, header := range []byte{0x11, 0x41, 0x51, 0x60, 0x6a, 0x71, 0x80, 0x8a, 0xa0, 0xaa, 0xc1, 0xe1, 0x36, 0x38} {
		t.Run(fmt.Sprintf("header_%02x", header), func(t *testing.T) {
			var frame []byte
			switch header >> 4 {
			case 1:
				frame = mqtt5ValidationConnect()
			case 3:
				frame = mqtt5ValidationFrame(0x30, 0, 1, 't', 0)
			case 4, 5, 6, 7:
				frame = mqtt5ValidationFrame(header, 0, 1)
			case 8:
				frame = mqtt5ValidationFrame(header, 0, 1, 0, 0, 1, 't', 0)
			case 10:
				frame = mqtt5ValidationFrame(header, 0, 1, 0, 0, 1, 't')
			default:
				frame = mqtt5ValidationFrame(header)
			}
			frame[0] = header
			if err := validateMQTT5Frame(frame); err == nil {
				t.Fatal("accepted reserved flags")
			}
		})
	}
	for option := 0; option < 256; option++ {
		frame := mqtt5ValidationFrame(0x82, 0, 1, 0, 0, 1, 't', byte(option))
		wantValid := option&0xc0 == 0 && option&3 != 3 && option>>4&3 != 3
		if err := validateMQTT5Frame(frame); (err == nil) != wantValid {
			t.Errorf("option %02x: error=%v want valid=%t", option, err, wantValid)
		}
	}
}

func TestMQTT5ValidationAcknowledgementReasons(t *testing.T) {
	for _, header := range []byte{0x40, 0x50, 0x62, 0x70} {
		for _, reason := range []byte{0, 0x10, 0x18, 0x80, 0x83, 0x87, 0x90, 0x91, 0x92, 0x97, 0x99, 0xff} {
			t.Run(fmt.Sprintf("%02x_%02x", header, reason), func(t *testing.T) {
				valid := reason == 0 || reason == 0x92
				if header == 0x40 || header == 0x50 {
					valid = bytes.Contains([]byte{0, 0x80, 0x83, 0x87, 0x90, 0x91, 0x97, 0x99}, []byte{reason})
				}
				err := validateMQTT5Frame(mqtt5ValidationFrame(header, 0, 1, reason))
				if (err == nil) != valid {
					t.Fatalf("error=%v want valid client acknowledgement=%t", err, valid)
				}
			})
		}
		if err := validateMQTT5Frame(mqtt5ValidationFrame(header, 0, 0)); err == nil {
			t.Errorf("%02x accepted zero identifier", header)
		}
	}
	if err := validateMQTT5Frame(mqtt5ValidationFrame(0xe0, 0xff)); err == nil {
		t.Error("accepted undefined DISCONNECT reason")
	}
}

func TestMQTT5ValidationRemainingLengthMatchesFrame(t *testing.T) {
	for _, frame := range [][]byte{{0xc0, 1}, {0x40, 0, 0, 1}, {0x30, 0, 0, 1, 't', 0}} {
		t.Run(fmt.Sprintf("%x", frame), func(t *testing.T) {
			if err := validateMQTT5Frame(frame); err == nil {
				t.Fatal("accepted mismatched Remaining Length")
			}
		})
	}
}

func TestMQTT5ValidationDisconnectReasonDirection(t *testing.T) {
	// MQTT 5 Table 3-10 distinguishes server-only reasons from bidirectional ones.
	clientReasons := []byte{0, 4, 0x80, 0x81, 0x82, 0x83, 0x90, 0x93, 0x94, 0x95, 0x96, 0x97, 0x98, 0x99}
	serverReasons := []byte{0x87, 0x89, 0x8b, 0x8d, 0x8e, 0x8f, 0x9a, 0x9b, 0x9c, 0x9d, 0x9e, 0x9f, 0xa0, 0xa1, 0xa2}
	for _, reason := range append(append([]byte(nil), clientReasons...), serverReasons...) {
		t.Run(fmt.Sprintf("%02x", reason), func(t *testing.T) {
			wantValid := bytes.Contains(clientReasons, []byte{reason})
			err := validateMQTT5Frame(mqtt5ValidationFrame(0xe0, reason, 0))
			if (err == nil) != wantValid {
				t.Fatalf("error=%v want valid client DISCONNECT=%t", err, wantValid)
			}
		})
	}
}

func TestMQTT5ValidationValidControls(t *testing.T) {
	for _, frame := range [][]byte{
		mqtt5ValidationConnect(),
		mqtt5ValidationConnect(0x26, 0, 0, 0, 0, 0x26, 0, 0, 0, 0),
		mqtt5ValidationFrame(0x30, 0, 1, 't', 0, 0xff, 0), // Payload is binary, not an MQTT string.
		mqtt5ValidationFrame(0x32, 0, 1, 't', 0, 1, 0),
		mqtt5ValidationFrame(0x40, 0, 1),
		mqtt5ValidationFrame(0x50, 0, 1, 0x87, 0),
		mqtt5ValidationFrame(0x62, 0, 1, 0x92),
		mqtt5ValidationFrame(0x70, 0, 1),
		mqtt5ValidationFrame(0x82, 0, 1, 0, 0, 1, '#', 0x2d),
		mqtt5ValidationFrame(0xa2, 0, 1, 0, 0, 1, '+'),
		mqtt5ValidationFrame(0xc0),
		mqtt5ValidationFrame(0xe0),
		mqtt5ValidationFrame(0xe0, 4),
	} {
		t.Run(fmt.Sprintf("%x", frame), func(t *testing.T) {
			if err := validateMQTT5Frame(frame); err != nil {
				t.Fatalf("rejected valid packet: %v", err)
			}
		})
	}
	if mqtt5Reason(io.ErrUnexpectedEOF) != 0x81 {
		t.Fatal("truncation must map to Malformed Packet")
	}
}
