package main

import (
	"bytes"
	"encoding/binary"
)

type WALEntry struct {
	LogSequenceNumber uint64
	Data              []byte
	CRC               uint32
	IsCheckpoint      bool
}

func (w WALEntry) Bytes() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, w)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
