// Package packets defines the broker packet domain while preserving legacy MQTT wire behavior.
package packets

import (
	"io"
	"time"

	mqtt5 "github.com/eclipse/paho.golang/packets"
	legacy "github.com/eclipse/paho.mqtt.golang/packets"
)

type (
	ControlPacket     = legacy.ControlPacket
	FixedHeader       = legacy.FixedHeader
	Details           = legacy.Details
	ConnectPacket     = legacy.ConnectPacket
	ConnackPacket     = legacy.ConnackPacket
	PubackPacket      = legacy.PubackPacket
	PubrecPacket      = legacy.PubrecPacket
	PubrelPacket      = legacy.PubrelPacket
	PubcompPacket     = legacy.PubcompPacket
	SubscribePacket   = legacy.SubscribePacket
	SubackPacket      = legacy.SubackPacket
	UnsubscribePacket = legacy.UnsubscribePacket
	UnsubackPacket    = legacy.UnsubackPacket
	PingreqPacket     = legacy.PingreqPacket
	PingrespPacket    = legacy.PingrespPacket
	DisconnectPacket  = legacy.DisconnectPacket
)

const (
	Connect                         = legacy.Connect
	Connack                         = legacy.Connack
	Publish                         = legacy.Publish
	Puback                          = legacy.Puback
	Pubrec                          = legacy.Pubrec
	Pubrel                          = legacy.Pubrel
	Pubcomp                         = legacy.Pubcomp
	Subscribe                       = legacy.Subscribe
	Suback                          = legacy.Suback
	Unsubscribe                     = legacy.Unsubscribe
	Unsuback                        = legacy.Unsuback
	Pingreq                         = legacy.Pingreq
	Pingresp                        = legacy.Pingresp
	Disconnect                      = legacy.Disconnect
	Accepted                        = legacy.Accepted
	ErrRefusedBadProtocolVersion    = legacy.ErrRefusedBadProtocolVersion
	ErrRefusedIDRejected            = legacy.ErrRefusedIDRejected
	ErrRefusedServerUnavailable     = legacy.ErrRefusedServerUnavailable
	ErrRefusedBadUsernameOrPassword = legacy.ErrRefusedBadUsernameOrPassword
	ErrRefusedNotAuthorised         = legacy.ErrRefusedNotAuthorised
	ErrNetworkError                 = legacy.ErrNetworkError
	ErrProtocolViolation            = legacy.ErrProtocolViolation
)

// PublishMetadata is broker-only state, immutable once published.
// CloneMetadata must be used before modifying metadata or its properties.
type PublishMetadata struct {
	Properties *mqtt5.Properties `json:",omitempty"`
	ExpiresAt  time.Time         `json:",omitzero"`
	Origin     string            `json:",omitempty"`
}

type PublishPacket struct {
	FixedHeader
	TopicName string
	MessageID uint16
	Payload   []byte
	Metadata  *PublishMetadata `json:",omitempty"`
}

var _ ControlPacket = (*PublishPacket)(nil)

func (p *PublishPacket) legacy() *legacy.PublishPacket {
	return &legacy.PublishPacket{FixedHeader: p.FixedHeader, TopicName: p.TopicName, MessageID: p.MessageID, Payload: p.Payload}
}

func (p *PublishPacket) String() string   { return p.legacy().String() }
func (p *PublishPacket) Details() Details { return p.legacy().Details() }
func (p *PublishPacket) Write(w io.Writer) error {
	old := p.legacy()
	err := old.Write(w)
	p.FixedHeader = old.FixedHeader
	return err
}
func (p *PublishPacket) Unpack(r io.Reader) error {
	old := p.legacy()
	err := old.Unpack(r)
	p.FixedHeader, p.TopicName, p.MessageID, p.Payload = old.FixedHeader, old.TopicName, old.MessageID, old.Payload
	return err
}

// Copy retains content, resets delivery state, and isolates metadata.
func (p *PublishPacket) Copy() *PublishPacket {
	return &PublishPacket{FixedHeader: FixedHeader{MessageType: Publish}, TopicName: p.TopicName, Payload: p.Payload, Metadata: CloneMetadata(p.Metadata)}
}

func NewControlPacket(packetType byte) ControlPacket {
	if packetType == Publish {
		return &PublishPacket{FixedHeader: FixedHeader{MessageType: Publish}}
	}
	return legacy.NewControlPacket(packetType)
}

func ReadPacket(r io.Reader) (ControlPacket, error) {
	p, err := legacy.ReadPacket(r)
	if old, ok := p.(*legacy.PublishPacket); ok {
		return &PublishPacket{FixedHeader: old.FixedHeader, TopicName: old.TopicName, MessageID: old.MessageID, Payload: old.Payload}, err
	}
	return p, err
}

// CloneMetadata returns an independent snapshot, including every mutable property.
func CloneMetadata(metadata *PublishMetadata) *PublishMetadata {
	if metadata == nil {
		return nil
	}
	result := *metadata
	if metadata.Properties == nil {
		return &result
	}
	properties := *metadata.Properties
	result.Properties = &properties
	properties.PayloadFormat = clonePointer(properties.PayloadFormat)
	properties.MessageExpiry = clonePointer(properties.MessageExpiry)
	properties.SubscriptionIdentifier = clonePointer(properties.SubscriptionIdentifier)
	properties.SessionExpiryInterval = clonePointer(properties.SessionExpiryInterval)
	properties.ServerKeepAlive = clonePointer(properties.ServerKeepAlive)
	properties.RequestProblemInfo = clonePointer(properties.RequestProblemInfo)
	properties.WillDelayInterval = clonePointer(properties.WillDelayInterval)
	properties.RequestResponseInfo = clonePointer(properties.RequestResponseInfo)
	properties.ReceiveMaximum = clonePointer(properties.ReceiveMaximum)
	properties.TopicAliasMaximum = clonePointer(properties.TopicAliasMaximum)
	properties.TopicAlias = clonePointer(properties.TopicAlias)
	properties.MaximumQOS = clonePointer(properties.MaximumQOS)
	properties.RetainAvailable = clonePointer(properties.RetainAvailable)
	properties.MaximumPacketSize = clonePointer(properties.MaximumPacketSize)
	properties.WildcardSubAvailable = clonePointer(properties.WildcardSubAvailable)
	properties.SubIDAvailable = clonePointer(properties.SubIDAvailable)
	properties.SharedSubAvailable = clonePointer(properties.SharedSubAvailable)
	properties.CorrelationData = cloneSlice(properties.CorrelationData)
	properties.AuthData = cloneSlice(properties.AuthData)
	properties.User = cloneSlice(properties.User)
	return &result
}

func clonePointer[T any](value *T) *T {
	if value == nil {
		return nil
	}
	result := *value
	return &result
}

func cloneSlice[T any](value []T) []T {
	if value == nil {
		return nil
	}
	result := make([]T, len(value))
	copy(result, value)
	return result
}
