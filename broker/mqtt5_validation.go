package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

const mqtt5MaximumPacketSize = 1 << 20
const mqtt5TopicAliasMaximum = 32

type mqtt5Error struct {
	reason  byte
	message string
}

func (e *mqtt5Error) Error() string { return e.message }

func mqtt5Reason(err error) byte {
	var e *mqtt5Error
	if errors.As(err, &e) {
		return e.reason
	}
	return 0x81
}

// Frame admission precedes library decoding, which allocates Remaining Length.
func readMQTTFrame(r io.Reader, maximum int) ([]byte, error) {
	var first [1]byte
	if _, err := io.ReadFull(r, first[:]); err != nil {
		return nil, err
	}
	frame := []byte{first[0]}
	length, multiplier := 0, 1
	for i := 0; i < 4; i++ {
		if _, err := io.ReadFull(r, first[:]); err != nil {
			return nil, err
		}
		b := first[0]
		frame = append(frame, b)
		length += int(b&127) * multiplier
		if b&128 == 0 {
			if i > 0 && b == 0 {
				return nil, &mqtt5Error{0x81, "noncanonical remaining length"}
			}
			if length+len(frame) > maximum {
				return nil, &mqtt5Error{0x95, "MQTT packet too large"}
			}
			header := len(frame)
			frame = append(frame, make([]byte, length)...)
			_, err := io.ReadFull(r, frame[header:])
			return frame, err
		}
		multiplier *= 128
	}
	return nil, &mqtt5Error{0x81, "remaining length exceeds four bytes"}
}

type mqtt5Cursor struct {
	data []byte
	pos  int
	err  error
}

func (c *mqtt5Cursor) take(n int) []byte {
	if c.err != nil {
		return nil
	}
	if n < 0 || n > len(c.data)-c.pos {
		c.err = io.ErrUnexpectedEOF
		return nil
	}
	p := c.data[c.pos : c.pos+n]
	c.pos += n
	return p
}
func (c *mqtt5Cursor) u8() byte {
	b := c.take(1)
	if b == nil {
		return 0
	}
	return b[0]
}
func (c *mqtt5Cursor) u16() uint16 {
	b := c.take(2)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint16(b)
}
func (c *mqtt5Cursor) u32() uint32 {
	b := c.take(4)
	if b == nil {
		return 0
	}
	return binary.BigEndian.Uint32(b)
}
func (c *mqtt5Cursor) vbi() int {
	n, m := 0, 1
	for i := 0; i < 4; i++ {
		b := c.u8()
		n += int(b&127) * m
		if c.err != nil {
			return 0
		}
		if b&128 == 0 {
			if i > 0 && b == 0 {
				c.fail("noncanonical variable integer")
			}
			return n
		}
		m *= 128
	}
	c.fail("variable integer exceeds four bytes")
	return 0
}
func (c *mqtt5Cursor) fail(s string) {
	if c.err == nil {
		c.err = &mqtt5Error{0x82, s}
	}
}
func validMQTTString(s string) bool { return utf8.ValidString(s) && !strings.ContainsRune(s, 0) }
func (c *mqtt5Cursor) str() string {
	n := int(c.u16())
	s := string(c.take(n))
	if !validMQTTString(s) {
		c.fail("invalid UTF-8 string")
	}
	return s
}
func (c *mqtt5Cursor) binary() { n := int(c.u16()); c.take(n) }
func validPublishTopic(s string) bool {
	return s != "" && validMQTTString(s) && !strings.ContainsAny(s, "+#")
}
func validSubscribeTopic(s string) bool {
	if strings.HasPrefix(s, "$share/") {
		p := strings.SplitN(s, "/", 3)
		if len(p) != 3 || p[1] == "" || strings.ContainsAny(p[1], "+#") {
			return false
		}
		s = p[2]
	}
	if s == "" || !validMQTTString(s) {
		return false
	}
	parts := strings.Split(s, "/")
	for i, p := range parts {
		if strings.ContainsAny(p, "+#") && (p != "+" && p != "#" || p == "#" && i != len(parts)-1) {
			return false
		}
	}
	return true
}

// Supplemental validation preserves evidence (duplicates/reserved bits) that
// the upstream client-oriented codec intentionally does not retain.
func (c *mqtt5Cursor) properties(kind byte, will bool) {
	n := c.vbi()
	data := c.take(n)
	if c.err != nil {
		return
	}
	p := mqtt5Cursor{data: data}
	seen := make(map[byte]bool)
	for p.pos < len(data) && p.err == nil {
		id := p.u8()
		allowed := false
		if will {
			allowed = strings.ContainsRune("\x01\x02\x03\x08\x09\x18\x26", rune(id))
		} else {
			switch kind {
			case 1:
				allowed = strings.ContainsRune("\x11\x15\x16\x17\x19\x21\x22\x26\x27", rune(id))
			case 3:
				allowed = strings.ContainsRune("\x01\x02\x03\x08\x09\x23\x26", rune(id))
			case 4, 5, 6, 7:
				allowed = id == 31 || id == 38
			case 8:
				allowed = id == 11 || id == 38
			case 10:
				allowed = id == 38
			case 14:
				allowed = id == 17 || id == 31 || id == 38
			}
		}
		if !allowed {
			p.fail(fmt.Sprintf("property %d not allowed", id))
			break
		}
		if id != 38 && seen[id] {
			p.fail("duplicate singleton property")
			break
		}
		seen[id] = true
		switch id {
		case 1, 23, 25:
			if p.u8() > 1 {
				p.fail("invalid boolean property")
			}
		case 2, 17, 24:
			p.u32()
		case 39:
			if p.u32() == 0 {
				p.fail("maximum packet size is zero")
			}
		case 33:
			if p.u16() == 0 {
				p.fail("receive maximum is zero")
			}
		case 34:
			p.u16()
		case 35:
			if alias := p.u16(); alias == 0 || alias > mqtt5TopicAliasMaximum {
				p.err = &mqtt5Error{0x94, "invalid topic alias"}
			}
		case 11:
			if p.vbi() == 0 {
				p.fail("subscription identifier is zero")
			}
		case 3, 31:
			p.str()
		case 8:
			if !validPublishTopic(p.str()) {
				p.fail("invalid response topic")
			}
		case 21:
			if p.str() == "" {
				p.fail("empty authentication method")
			}
		case 9, 22:
			p.binary()
		case 38:
			p.str()
			p.str()
		}
	}
	if p.err != nil {
		c.err = p.err
	}
}

func validateMQTT5Frame(frame []byte) error {
	c := mqtt5Cursor{data: frame}
	header := c.u8()
	kind, flags := header>>4, header&15
	remaining := c.vbi()
	if c.err == nil && remaining != len(frame)-c.pos {
		c.fail("remaining length does not match frame")
	}
	if kind != 3 {
		expected := byte(0)
		if kind == 6 || kind == 8 || kind == 10 {
			expected = 2
		}
		if flags != expected {
			c.fail("invalid fixed header flags")
		}
	}
	identifier := func() {
		if c.u16() == 0 {
			c.fail("packet identifier is zero")
		}
	}
	switch kind {
	case 1:
		if c.str() != "MQTT" || c.u8() != 5 {
			c.fail("invalid MQTT 5 protocol header")
		}
		f := c.u8()
		c.u16()
		if f&1 != 0 || f&4 == 0 && f&0x38 != 0 || (f>>3)&3 == 3 {
			c.fail("invalid CONNECT flags")
		}
		c.properties(kind, false)
		c.str()
		if f&4 != 0 {
			c.properties(kind, true)
			if !validPublishTopic(c.str()) {
				c.fail("invalid will topic")
			}
			c.binary()
		}
		if f&128 != 0 {
			c.str()
		}
		if f&64 != 0 {
			c.binary()
		}
	case 3:
		qos := flags >> 1 & 3
		if qos == 3 || qos == 0 && flags&8 != 0 {
			c.fail("invalid PUBLISH flags")
		}
		topic := c.str()
		if topic != "" && !validPublishTopic(topic) {
			c.err = &mqtt5Error{0x90, "invalid topic name"}
		}
		if qos > 0 {
			identifier()
		}
		c.properties(kind, false)
		c.pos = len(frame)
	case 4, 5, 6, 7:
		identifier()
		if c.pos < len(frame) {
			r := c.u8()
			allowed := false
			switch kind {
			case 4, 5:
				allowed = strings.IndexByte("\x00\x80\x83\x87\x90\x91\x97\x99", r) >= 0
			case 6, 7:
				allowed = r == 0 || r == 0x92
			}
			if !allowed {
				c.fail("invalid acknowledgement reason")
			}
			if c.pos < len(frame) {
				c.properties(kind, false)
			}
		}
	case 8:
		identifier()
		c.properties(kind, false)
		count := 0
		for c.pos < len(frame) && c.err == nil {
			topic := c.str()
			option := c.u8()
			count++
			if !validSubscribeTopic(topic) {
				c.fail("invalid topic filter")
			}
			if option&0xc0 != 0 || option&3 == 3 || option>>4&3 == 3 || option&4 != 0 && strings.HasPrefix(topic, "$share/") {
				c.fail("invalid subscription options")
			}
		}
		if count == 0 {
			c.fail("empty SUBSCRIBE")
		}
	case 10:
		identifier()
		c.properties(kind, false)
		count := 0
		for c.pos < len(frame) && c.err == nil {
			if !validSubscribeTopic(c.str()) {
				c.fail("invalid topic filter")
			}
			count++
		}
		if count == 0 {
			c.fail("empty UNSUBSCRIBE")
		}
	case 12:
	case 14:
		if c.pos < len(frame) {
			r := c.u8()
			if strings.IndexByte("\x00\x04\x80\x81\x82\x83\x90\x93\x94\x95\x96\x97\x98\x99", r) < 0 {
				c.fail("invalid disconnect reason")
			}
			if c.pos < len(frame) {
				c.properties(kind, false)
			}
		}
	default:
		c.fail("packet not permitted from client")
	}
	if c.err == nil && c.pos != len(frame) {
		c.fail("trailing packet data")
	}
	return c.err
}
