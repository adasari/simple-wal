package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	SEGMENT_FILE_PREFIX = "wal-"
	SYNC_INTERVAL       = 100 * time.Millisecond
)

type WAL struct {
	directory      string
	currentSegment *SegmentFile
	syncTimer      *time.Timer
	lock           sync.Mutex
	lastSequenceNo uint64
	bufferWriter   *bufio.Writer
	maxFileSize    int64
	maxSegments    int
	ctx            context.Context
	cancel         context.CancelFunc
	shouldFsync    bool
}

func OpenWAL(directory string, enableFSync bool, maxSegments int, maxFileSize int64) (*WAL, error) {
	// create directory if not exist.
	if err := os.MkdirAll(directory, 0755); err != nil {
		return nil, err
	}

	// read all files and get last segment
	segmentFiles, err := filepath.Glob(filepath.Join(directory, SEGMENT_FILE_PREFIX+"*"))
	if err != nil {
		return nil, err
	}

	lastSegmentID := 0
	if len(segmentFiles) > 0 {
		for _, f := range segmentFiles {
			_, fileName := filepath.Split(f)
			segmentID, err := strconv.Atoi(strings.TrimPrefix(fileName, SEGMENT_FILE_PREFIX))
			if err != nil {
				return nil, err
			}
			if segmentID > lastSegmentID {
				lastSegmentID = segmentID
			}
		}
	}

	segmentFile, err := NewSegmentFile(directory, uint(lastSegmentID))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	wal := &WAL{
		maxSegments:    maxSegments,
		directory:      directory,
		currentSegment: segmentFile,
		bufferWriter:   bufio.NewWriter(segmentFile.File()),
		syncTimer:      time.NewTimer(SYNC_INTERVAL),
		shouldFsync:    enableFSync,
		ctx:            ctx,
		cancel:         cancel,
		maxFileSize:    maxFileSize,
		lastSequenceNo: segmentFile.LastLSN,
	}

	// flush the buffer at regular intervals.
	go wal.keepSyncing()

	return wal, nil
}

// WriteEntry writes an entry to the WAL.
func (wal *WAL) WriteEntry(data []byte) error {
	return wal.writeEntry(data, false)
}

// CreateCheckpoint creates a checkpoint entry in the WAL. A checkpoint entry
// is a special entry that can be used to restore the state of the system to
// the point when the checkpoint was created. it force flushes the current segment file.
func (wal *WAL) CreateCheckpoint(data []byte) error {
	return wal.writeEntry(data, true)
}

func (wal *WAL) writeEntry(data []byte, isCheckpoint bool) error {
	wal.lock.Lock()
	defer wal.lock.Unlock()

	if err := wal.rotateLogIfNeeded(); err != nil {
		return err
	}

	if isCheckpoint {
		if err := wal.Sync(); err != nil {
			return fmt.Errorf("could not create checkpoint, error while syncing: %v", err)
		}
	}

	wal.lastSequenceNo++
	crcData := append(data, byte(wal.lastSequenceNo))

	entry := &WALEntry{
		LogSequenceNumber: wal.lastSequenceNo,
		Data:              data,
		CRC:               crc32.ChecksumIEEE(crcData),
		IsCheckpoint:      isCheckpoint,
	}

	return wal.writeEntryToBuffer(entry)
}

func (wal *WAL) writeEntryToBuffer(entry *WALEntry) error {
	data, err := entry.Bytes()
	if err != nil {
		return err
	}

	size := int32(len(data))
	if err := binary.Write(wal.bufferWriter, binary.LittleEndian, size); err != nil {
		return err
	}

	_, err = wal.bufferWriter.Write(data)
	return err
}

func (wal *WAL) rotateLogIfNeeded() error {
	fileInfo, err := wal.currentSegment.File().Stat()
	if err != nil {
		return err
	}

	if fileInfo.Size()+int64(wal.bufferWriter.Buffered()) >= wal.maxFileSize {
		if err := wal.rotateLog(); err != nil {
			return err
		}
	}

	return nil
}

func (wal *WAL) rotateLog() error {
	if err := wal.Sync(); err != nil {
		return err
	}

	// close the current segment file
	if err := wal.currentSegment.Close(); err != nil {
		return err
	}

	segmentID := wal.currentSegment.ID
	// purge old segment file
	if int(segmentID) >= wal.maxSegments {
		if err := wal.deleteOldestSegment(); err != nil {
			return err
		}
	}

	// create new segment and attach to wal.
	newSegmentFile, err := NewSegmentFile(wal.directory, segmentID+1)
	if err != nil {
		return err
	}

	wal.currentSegment = newSegmentFile
	wal.bufferWriter = bufio.NewWriter(newSegmentFile.File())

	return nil
}

func (wal *WAL) deleteOldestSegment() error {
	files, err := filepath.Glob(filepath.Join(wal.directory, SEGMENT_FILE_PREFIX+"*"))
	if err != nil {
		return err
	}

	var oldestSegmentFilePath string
	if len(files) > 0 {
		// Find the oldest segment ID
		oldestSegmentID := math.MaxInt64
		for _, file := range files {
			// Get the segment index from the file name
			segmentIndex, err := strconv.Atoi(strings.TrimPrefix(file, filepath.Join(wal.directory, SEGMENT_FILE_PREFIX)))
			if err != nil {
				return err
			}

			if segmentIndex < oldestSegmentID {
				oldestSegmentID = segmentIndex
				oldestSegmentFilePath = file
			}
		}
	} else {
		return nil
	}

	// Delete the oldest segment file
	if err := os.Remove(oldestSegmentFilePath); err != nil {
		return err
	}

	return nil
}

func (wal *WAL) Sync() error {
	if err := wal.bufferWriter.Flush(); err != nil {
		return err
	}
	if wal.shouldFsync {
		if err := wal.currentSegment.File().Sync(); err != nil {
			return err
		}
	}

	// Reset the keepSyncing timer, since we just synced.
	wal.resetTimer()

	return nil
}

// Close the WAL file. It also calls Sync() on the WAL.
func (wal *WAL) Close() error {
	wal.cancel()
	if err := wal.Sync(); err != nil {
		return err
	}
	return wal.currentSegment.Close()
}

// resetTimer resets the synchronization timer.
func (wal *WAL) resetTimer() {
	wal.syncTimer.Reset(SYNC_INTERVAL)
}

func (wal *WAL) keepSyncing() {
	for {
		select {
		case <-wal.syncTimer.C:

			wal.lock.Lock()
			err := wal.Sync()
			wal.lock.Unlock()

			if err != nil {
				log.Printf("Error while performing sync: %v", err)
			}

		case <-wal.ctx.Done():
			return
		}
	}
}
