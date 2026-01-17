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
	ErrChecksum = errors.New("wal: invalid checksum")
	ErrClosed   = errors.New("wal: writer is closed")
)

const (
	// initialBufferSize is the initial capacity for the reusable write buffer
	// This reduces allocations for small writes
	initialBufferSize = 512
	// headerSize is the fixed size of WAL record header
	headerSize = 12
	// initialDataBufferSize is the initial capacity for the reusable data buffer in Load
	initialDataBufferSize = 1024
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

func (w *WalWriter) Load(apply func(k, v []byte)) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return ErrClosed
	}

	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	for {
		// Reuse header buffer (fixed size)
		_, err := io.ReadFull(w.file, w.headerBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		expectSum := binary.LittleEndian.Uint32(w.headerBuf[0:4])
		ksiz := binary.LittleEndian.Uint32(w.headerBuf[4:8])
		vsiz := binary.LittleEndian.Uint32(w.headerBuf[8:12])

		// Reuse data buffer, grow if needed
		neededSize := int(ksiz + vsiz)
		if cap(w.dataBuf) < neededSize {
			w.dataBuf = make([]byte, neededSize)
		}
		data := w.dataBuf[:neededSize]

		if _, err := io.ReadFull(w.file, data); err != nil {
			return err
		}

		// check sum
		actualSum := crc32.ChecksumIEEE(w.headerBuf[4:])
		actualSum = crc32.Update(actualSum, crc32.IEEETable, data)
		if expectSum != actualSum {
			return ErrChecksum
		}

		// restore data
		key := data[:ksiz]
		value := data[ksiz:]

		// handle tombstone
		if vsiz == 0 {
			apply(key, nil)
		} else {
			apply(key, value)
		}
	}

	return nil
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
