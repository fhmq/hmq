package packets

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	mqtt5 "github.com/eclipse/paho.golang/packets"
	legacy "github.com/eclipse/paho.mqtt.golang/packets"
)

func TestLegacyWireRoundTrip(t *testing.T) {
	for qos := byte(0); qos <= 2; qos++ {
		p := NewControlPacket(Publish).(*PublishPacket)
		p.Qos, p.Dup, p.Retain = qos, true, true
		p.TopicName, p.MessageID, p.Payload = "test/topic", 42, []byte{0, 1, 255}
		p.Metadata = &PublishMetadata{Origin: "local"}
		old := p.legacy()
		var got, want bytes.Buffer
		if err := old.Write(&want); err != nil {
			t.Fatal(err)
		}
		if err := p.Write(&got); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Bytes(), want.Bytes()) || p.FixedHeader != old.FixedHeader {
			t.Fatalf("qos %d: wire/header mismatch", qos)
		}
		if p.String() != old.String() || p.Details() != old.Details() {
			t.Fatal("delegation mismatch")
		}
		decoded, err := ReadPacket(bytes.NewReader(want.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		publish, ok := decoded.(*PublishPacket)
		if !ok {
			t.Fatalf("unexpected packet %T", decoded)
		}
		expected := *p
		expected.Metadata = nil
		if qos == 0 {
			expected.MessageID = 0
		}
		if !reflect.DeepEqual(publish, &expected) {
			t.Fatalf("round trip: %#v", publish)
		}
		decodedLegacy, err := legacy.ReadPacket(bytes.NewReader(got.Bytes()))
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(decodedLegacy, publish.legacy()) {
			t.Fatal("legacy decode mismatch")
		}
	}
}

func TestUnpackDelegates(t *testing.T) {
	for _, body := range [][]byte{{0}, {0, 1, 'a'}, {0, 1, 'a', 0, 9, 'x'}} {
		p := &PublishPacket{FixedHeader: FixedHeader{MessageType: Publish, Qos: 1, RemainingLength: 6}, Metadata: &PublishMetadata{Origin: "local"}}
		old := p.legacy()
		wantErr := old.Unpack(bytes.NewReader(body))
		gotErr := p.Unpack(bytes.NewReader(body))
		if (gotErr == nil) != (wantErr == nil) || !reflect.DeepEqual(p.legacy(), old) {
			t.Fatal("unpack behavior differs")
		}
		if p.Metadata.Origin != "local" {
			t.Fatal("metadata changed")
		}
	}
}

func TestNonPublishAndReadError(t *testing.T) {
	for kind := byte(Connect); kind <= Disconnect; kind++ {
		if kind == Publish {
			continue
		}
		if reflect.TypeOf(NewControlPacket(kind)) != reflect.TypeOf(legacy.NewControlPacket(kind)) {
			t.Fatalf("type changed: %d", kind)
		}
	}
	var wire bytes.Buffer
	if err := NewControlPacket(Pingreq).Write(&wire); err != nil {
		t.Fatal(err)
	}
	p, err := ReadPacket(&wire)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.(*PingreqPacket); !ok {
		t.Fatalf("unexpected %T", p)
	}
	if p, err := ReadPacket(bytes.NewReader(nil)); p != nil || !errors.Is(err, io.EOF) {
		t.Fatalf("got %v, %v", p, err)
	}
}

func TestCloneMetadata(t *testing.T) {
	if CloneMetadata(nil) != nil {
		t.Fatal("nil metadata changed")
	}
	empty := &PublishMetadata{Origin: "local"}
	if got := CloneMetadata(empty); got == empty || !reflect.DeepEqual(got, empty) {
		t.Fatal("empty clone mismatch")
	}
	properties := &mqtt5.Properties{}
	// Populate all pointer and slice fields so newly added mutable fields also get checked.
	value := reflect.ValueOf(properties).Elem()
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		switch field.Kind() {
		case reflect.Pointer:
			field.Set(reflect.New(field.Type().Elem()))
		case reflect.Slice:
			field.Set(reflect.MakeSlice(field.Type(), 1, 1))
		case reflect.String:
			field.SetString("original")
		}
	}
	metadata := &PublishMetadata{Properties: properties, Origin: "local", ExpiresAt: time.Now()}
	clone := CloneMetadata(metadata)
	if clone == metadata || clone.Properties == properties || !reflect.DeepEqual(clone, metadata) {
		t.Fatal("clone mismatch")
	}
	clonedFields := reflect.ValueOf(clone.Properties).Elem()
	for i := 0; i < value.NumField(); i++ {
		original, copied := value.Field(i), clonedFields.Field(i)
		if (original.Kind() == reflect.Pointer || original.Kind() == reflect.Slice) && original.Pointer() == copied.Pointer() {
			t.Errorf("aliased property: %s", value.Type().Field(i).Name)
		}
	}
	*clone.Properties.MessageExpiry = 99
	clone.Properties.CorrelationData[0] = 9
	clone.Properties.AuthData[0] = 8
	clone.Properties.User[0].Key = "changed"
	if *properties.MessageExpiry != 0 || properties.CorrelationData[0] != 0 || properties.AuthData[0] != 0 || properties.User[0].Key != "" {
		t.Fatal("clone mutated original")
	}
	if !reflect.DeepEqual(CloneMetadata(&PublishMetadata{Properties: &mqtt5.Properties{}}).Properties, &mqtt5.Properties{}) {
		t.Fatal("nil property fields changed")
	}
}

func TestCopyAndJSON(t *testing.T) {
	p := &PublishPacket{
		FixedHeader: FixedHeader{MessageType: Publish, Qos: 2, Dup: true, Retain: true, RemainingLength: 10},
		TopicName:   "topic", MessageID: 42, Payload: []byte("payload"),
		Metadata: &PublishMetadata{Origin: "local", Properties: &mqtt5.Properties{User: []mqtt5.User{{Key: "key", Value: "value"}}}},
	}
	copied := p.Copy()
	if copied.FixedHeader != (FixedHeader{MessageType: Publish}) || copied.MessageID != 0 || copied.TopicName != p.TopicName || !bytes.Equal(copied.Payload, p.Payload) {
		t.Fatal("copy contract changed")
	}
	if copied.Metadata == p.Metadata || copied.Metadata.Properties == p.Metadata.Properties {
		t.Fatal("metadata alias")
	}
	copied.Metadata.Properties.User[0].Value = "changed"
	if p.Metadata.Properties.User[0].Value != "value" {
		t.Fatal("properties alias")
	}
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var decoded PublishPacket
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(&decoded, p) {
		t.Fatal("JSON round trip mismatch")
	}
	p.Metadata = nil
	data, err = json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("Metadata")) {
		t.Fatal("nil metadata not omitted")
	}
	old, err := json.Marshal(p.legacy())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, old) {
		t.Fatal("legacy JSON changed")
	}
}
