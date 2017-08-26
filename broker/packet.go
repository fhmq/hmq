package broker

import (
	"errors"
	"hmq/lib/message"
	"io"
	"net"

	log "github.com/cihub/seelog"
)

func checkError(desc string, err error) {
	if err != nil {
		log.Error(desc, " : ", err)
	}
}

func ReadPacket(conn net.Conn) ([]byte, error) {
	if conn == nil {
		return nil, errors.New("conn is null")
	}
	// conn.SetReadDeadline(t)
	var buf []byte
	// read fix header
	b := make([]byte, 1)
	_, err := io.ReadFull(conn, b)
	if err != nil {
		return nil, err
	}
	buf = append(buf, b...)
	// read rem msg length
	rembuf, remlen := decodeLength(conn)
	buf = append(buf, rembuf...)
	// read rem msg
	packetBytes := make([]byte, remlen)
	_, err = io.ReadFull(conn, packetBytes)
	if err != nil {
		return nil, err
	}
	buf = append(buf, packetBytes...)
	// log.Info("len buf: ", len(buf))
	return buf, nil
}

func decodeLength(r io.Reader) ([]byte, int) {
	var rLength uint32
	var multiplier uint32
	var buf []byte
	b := make([]byte, 1)
	for {
		io.ReadFull(r, b)
		digit := b[0]
		buf = append(buf, b[0])
		rLength |= uint32(digit&127) << multiplier
		if (digit & 128) == 0 {
			break
		}
		multiplier += 7

	}
	return buf, int(rLength)
}

func DecodeMessage(buf []byte) (message.Message, error) {
	msgType := uint8(buf[0] & 0xF0 >> 4)
	switch msgType {
	case CONNECT:
		return DecodeConnectMessage(buf)
	case CONNACK:
		return DecodeConnackMessage(buf)
	case PUBLISH:
		return DecodePublishMessage(buf)
	case PUBACK:
		return DecodePubackMessage(buf)
	case PUBCOMP:
		return DecodePubcompMessage(buf)
	case PUBREC:
		return DecodePubrecMessage(buf)
	case PUBREL:
		return DecodePubrelMessage(buf)
	case SUBSCRIBE:
		return DecodeSubscribeMessage(buf)
	case SUBACK:
		return DecodeSubackMessage(buf)
	case UNSUBSCRIBE:
		return DecodeUnsubscribeMessage(buf)
	case UNSUBACK:
		return DecodeUnsubackMessage(buf)
	case PINGREQ:
		return DecodePingreqMessage(buf)
	case PINGRESP:
		return DecodePingrespMessage(buf)
	case DISCONNECT:
		return DecodeDisconnectMessage(buf)
	default:
		return nil, errors.New("error message type")
	}
}

func DecodeConnectMessage(buf []byte) (*message.ConnectMessage, error) {
	connMsg := message.NewConnectMessage()
	_, err := connMsg.Decode(buf)
	if err != nil {
		if !message.ValidConnackError(err) {
			return nil, errors.New("Connect message format error, " + err.Error())
		}
		return nil, errors.New("Deode connect message error, " + err.Error())
	}
	return connMsg, nil
}

func DecodeConnackMessage(buf []byte) (*message.ConnackMessage, error) {
	msg := message.NewConnackMessage()
	_, err := msg.Decode(buf)
	if err != nil {
		return nil, errors.New("Decode Connack message error, " + err.Error())
	}
	return msg, nil
}

func DecodePublishMessage(buf []byte) (*message.PublishMessage, error) {
	msg := message.NewPublishMessage()
	_, err := msg.Decode(buf)
	if err != nil {
		return nil, errors.New("Decode Publish message error, " + err.Error())
	}
	return msg, nil
}

func DecodePubackMessage(buf []byte) (*message.PubackMessage, error) {
	msg := message.NewPubackMessage()
	_, err := msg.Decode(buf)
	if err != nil {
		return nil, errors.New("Decode Puback message error, " + err.Error())
	}
	return msg, nil
}

func DecodePubrecMessage(buf []byte) (*message.PubrecMessage, error) {
	msg := message.NewPubrecMessage()
	_, err := msg.Decode(buf)
	if err != nil {
		return nil, errors.New("Decode Pubrec message error, " + err.Error())
	}
	return msg, nil
}

func DecodePubrelMessage(buf []byte) (*message.PubrelMessage, error) {
	msg := message.NewPubrelMessage()
	_, err := msg.Decode(buf)
	if err != nil {
		return nil, errors.New("Decode Pubrel message error, " + err.Error())
	}
	return msg, nil
}

func DecodePubcompMessage(buf []byte) (*message.PubcompMessage, error) {
	msg := message.NewPubcompMessage()
	_, err := msg.Decode(buf)
	if err != nil {
		return nil, errors.New("Decode Pubcomp message error, " + err.Error())
	}
	return msg, nil
}

func DecodeSubscribeMessage(buf []byte) (*message.SubscribeMessage, error) {
	msg := message.NewSubscribeMessage()
	_, err := msg.Decode(buf)
	if err != nil {
		return nil, errors.New("Decode Subscribe message error, " + err.Error())
	}
	return msg, nil
}

func DecodeSubackMessage(buf []byte) (*message.SubackMessage, error) {
	msg := message.NewSubackMessage()
	_, err := msg.Decode(buf)
	if err != nil {
		return nil, errors.New("Decode Suback message error, " + err.Error())
	}
	return msg, nil
}

func DecodeUnsubscribeMessage(buf []byte) (*message.UnsubscribeMessage, error) {
	msg := message.NewUnsubscribeMessage()
	_, err := msg.Decode(buf)
	if err != nil {
		return nil, errors.New("Decode Unsubscribe message error, " + err.Error())
	}
	return msg, nil
}

func DecodeUnsubackMessage(buf []byte) (*message.UnsubackMessage, error) {
	msg := message.NewUnsubackMessage()
	_, err := msg.Decode(buf)
	if err != nil {
		return nil, errors.New("Decode Unsuback message error, " + err.Error())
	}
	return msg, nil
}

func DecodePingreqMessage(buf []byte) (*message.PingreqMessage, error) {
	msg := message.NewPingreqMessage()
	_, err := msg.Decode(buf)
	if err != nil {
		return nil, errors.New("Decode Pingreq message error, " + err.Error())
	}
	return msg, nil
}

func DecodePingrespMessage(buf []byte) (*message.PingrespMessage, error) {
	msg := message.NewPingrespMessage()
	_, err := msg.Decode(buf)
	if err != nil {
		return nil, errors.New("Decode Pingresp message error, " + err.Error())
	}
	return msg, nil
}

func DecodeDisconnectMessage(buf []byte) (*message.DisconnectMessage, error) {
	msg := message.NewDisconnectMessage()
	_, err := msg.Decode(buf)
	if err != nil {
		return nil, errors.New("Decode Disconnect message error, " + err.Error())
	}
	return msg, nil
}

func EncodeMessage(msg message.Message) ([]byte, error) {
	buf := make([]byte, msg.Len())
	_, err := msg.Encode(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}
