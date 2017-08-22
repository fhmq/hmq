package broker

import (
	"errors"
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
