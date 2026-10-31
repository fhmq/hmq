package broker

import (
	"bytes"
	"testing"

	mqtt5 "github.com/eclipse/paho.golang/packets"
)

func FuzzMQTT5ValidatedDecode(f *testing.F) {
	f.Add([]byte{0xc0, 0})
	f.Add([]byte{0x10, 14, 0, 4, 'M', 'Q', 'T', 'T', 5, 2, 0, 0, 0, 0, 1, 'c'})
	f.Add([]byte{0x30, 4, 0, 1, 't', 0})
	f.Add([]byte{0x82, 7, 0, 1, 0, 0, 1, 't', 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > mqtt5MaximumPacketSize || validateMQTT5Frame(data) != nil {
			return
		}
		// Any accepted frame must be safe for the upstream client-oriented decoder.
		_, _ = mqtt5.ReadPacket(bytes.NewReader(data))
	})
}
