package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

var (
	ErrChecksum    = errors.New("wal: invalid checksum")
	ErrClosed      = errors.New("wal: writer is closed")
	ErrInvalidSize = errors.New("wal: invalid key or value size")
)

const (
	// initialBufferSize is the initial capacity for the reusable write buffer
	// This reduces allocations for small writes
	initialBufferSize = 512
	// headerSize is the fixed size of WAL record header
	headerSize = 12
	// initialDataBufferSize is the initial capacity for the reusable data buffer in Load
	initialDataBufferSize = 1024
	// maxKeySize is the maximum allowed key size (1MB)
	maxKeySize = 1 << 20
	// maxValueSize is the maximum allowed value size (10MB)
	maxValueSize = 10 << 20
	// maxRecordSize is the maximum allowed total record size (header + key + value)
	maxRecordSize = headerSize + maxKeySize + maxValueSize
)

// Write-Ahead Log implementation
type WalWriter struct {
	mu        sync.Mutex
	file      *os.File
	buf       []byte // reusable buffer for Write operations
	headerBuf []byte // reusable buffer for Load header (fixed 12 bytes)
	dataBuf   []byte // reusable buffer for Load data (grows as needed)
}

func NewWalWriter(path string) (*WalWriter, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WalWriter{
		file:      f,
		buf:       make([]byte, 0, initialBufferSize),     // pre-allocate write buffer capacity
		headerBuf: make([]byte, headerSize),               // fixed-size header buffer
		dataBuf:   make([]byte, 0, initialDataBufferSize), // pre-allocate data buffer capacity
	}, nil
}

func (w *WalWriter) Write(key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return ErrClosed
	}

	ksiz := len(key)
	vsiz := len(value)
	// Header(12) = CheckSum(4) + kSize(4) + VSize(4)
	neededSize := 12 + ksiz + vsiz

	// Reuse buffer if capacity is sufficient, otherwise grow it
	if cap(w.buf) < neededSize {
		w.buf = make([]byte, neededSize)
	}
	buf := w.buf[:neededSize]

	binary.LittleEndian.PutUint32(buf[4:8], uint32(ksiz))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(vsiz))

	copy(buf[12:], key)
	copy(buf[12+ksiz:], value)

	sum := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], sum)

	_, err := w.file.Write(buf)
	return err
}

// file.Write only writes to Page Cache in Kernel
// fsync forces swap data in cache into disk
func (w *WalWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return ErrClosed
	}
	return w.file.Sync()
}

// LoadResult contains statistics about the Load operation
type LoadResult struct {
	Recovered int // number of records successfully recovered
	Skipped   int // number of corrupted records skipped
}

// Load restores data from WAL file with fault tolerance
// It skips corrupted records and continues recovery instead of stopping
// Returns LoadResult with recovery statistics
func (w *WalWriter) Load(apply func(k, v []byte)) (*LoadResult, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil, ErrClosed
	}

	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, err
	}

	result := &LoadResult{}

	for {
		// Reuse header buffer (fixed size)
		_, err := io.ReadFull(w.file, w.headerBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			// If we can't read header, we've reached end or file is corrupted
			// Try to continue from next potential record boundary
			// For now, we'll break and return partial recovery
			break
		}

		expectSum := binary.LittleEndian.Uint32(w.headerBuf[0:4])
		ksiz := binary.LittleEndian.Uint32(w.headerBuf[4:8])
		vsiz := binary.LittleEndian.Uint32(w.headerBuf[8:12])

		// Security: Validate sizes to prevent memory exhaustion attacks
		if ksiz > maxKeySize || vsiz > maxValueSize {
			// Invalid size, skip this record
			result.Skipped++
			// Try to find next record by seeking forward
			// For simplicity, we'll break here (could implement more sophisticated recovery)
			break
		}

		neededSize := int(ksiz + vsiz)
		if neededSize > maxRecordSize-headerSize {
			result.Skipped++
			break
		}

		// Reuse data buffer, grow if needed
		if cap(w.dataBuf) < neededSize {
			w.dataBuf = make([]byte, neededSize)
		}
		data := w.dataBuf[:neededSize]

		if _, err := io.ReadFull(w.file, data); err != nil {
			// Can't read data, skip this record
			result.Skipped++
			break
		}

		// Verify checksum
		actualSum := crc32.ChecksumIEEE(w.headerBuf[4:])
		actualSum = crc32.Update(actualSum, crc32.IEEETable, data)
		if expectSum != actualSum {
			// Checksum mismatch, skip this corrupted record
			result.Skipped++
			// Continue to next record instead of stopping
			continue
		}

		// Checksum valid, restore data
		key := data[:ksiz]
		value := data[ksiz:]

		// handle tombstone
		if vsiz == 0 {
			apply(key, nil)
		} else {
			apply(key, value)
		}
		result.Recovered++
	}

	return result, nil
}

// Close closes the WAL file
// After closing, all operations will return ErrClosed
func (w *WalWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil // already closed
	}

	err := w.file.Close()
	w.file = nil // mark as closed
	return err
}
