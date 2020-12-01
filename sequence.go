package YTDNMgmt

import (
	"sync/atomic"
	"time"
)

var ID_SEQ *int32 = new(int32)

func GetSequence(snid int32) int64 {
	id := atomic.AddInt32(ID_SEQ, 1)
	bs := make([]byte, 4)
	bs[0] = uint8(snid)
	bs[1] = uint8(id >> 16)
	bs[2] = uint8(id >> 8)
	bs[3] = uint8(id)
	vbi := int32(bs[0] & 0xFF)
	vbi = vbi<<8 | int32(bs[1]&0xFF)
	vbi = vbi<<8 | int32(bs[2]&0xFF)
	vbi = vbi<<8 | int32(bs[3]&0xFF)
	h := time.Now().Unix()
	l := int64(vbi - 1)
	high := (h & 0x000000ffffffff) << 32
	low := l & 0x00000000ffffffff
	return high | low
}
