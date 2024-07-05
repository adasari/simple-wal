package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type SegmentFile struct {
	ID      uint
	file    *os.File
	LastLSN uint64
}

func NewSegmentFile(directory string, segmentID uint) (*SegmentFile, error) {
	filePath := filepath.Join(directory, SEGMENT_FILE_PREFIX+fmt.Sprintf("%d", segmentID))
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	// file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	file.Seek(0, io.SeekEnd)

	return &SegmentFile{
		file: file,
		ID:   segmentID,
	}, nil
}

func (s SegmentFile) LastEntryInLog() (*WALEntry, error) {
	return nil, nil
}

func (s SegmentFile) File() *os.File {
	return nil
}

func (s SegmentFile) Close() error {
	return s.file.Close()
}
