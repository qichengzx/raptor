package server

import "encoding/binary"

func bytesToUint32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func uint32ToBytes(size, u uint32) []byte {
	buf := make([]byte, size)
	binary.BigEndian.PutUint32(buf, u)
	return buf
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func uint64ToBytes(size, u uint64) []byte {
	buf := make([]byte, size)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}
