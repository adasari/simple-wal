package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type OpCode int

const (
	Insert OpCode = iota
	Update
	Delete
)

type Record struct {
	Op    OpCode `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

func main() {

	dirPath := "wal.log"
	defer os.RemoveAll(dirPath)

	maxFileSize := 10 * 1024 * 1024 // 10 mb
	maxSegments := int64(5)
	// opening the wal.
	walog, err := OpenWAL(dirPath, true, maxFileSize, maxSegments)
	if err != nil {
		fmt.Printf("Failed to open wal: %v\n", err)
	}

	record := Record{
		Op:    Insert,
		Key:   "test",
		Value: []byte("value"),
	}

	b, err := json.Marshal(record)
	if err != nil {
		fmt.Printf("failed to marshal record: %v", err)
	}
	// adding entry to wal.
	walog.WriteEntry(b)

	// close wal.
	walog.Close()
}
