package pb

import (
	"bytes"
	"encoding/binary"
)

const (
	MsgIDDownloadShardRequest  msgType = 0x1757
	MsgIDDownloadShardResponse msgType = 0x7a56
)

type msgType int32

func (mt msgType) Bytes() []byte {
	buf := bytes.NewBuffer([]byte{})
	binary.Write(buf, binary.BigEndian, int16(mt))
	return buf.Bytes()
}
func (mt msgType) Value() int32 {
	return int32(mt)
}
