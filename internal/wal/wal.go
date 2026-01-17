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

// Write-Ahead Log implementation
type WalWriter struct {
	mu   sync.Mutex
	file *os.File
}

func NewWalWriter(path string) (*WalWriter, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &WalWriter{file: f}, nil
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
	buf := make([]byte, 12+ksiz+vsiz)

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
		header := make([]byte, 12)
		_, err := io.ReadFull(w.file, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		expectSum := binary.LittleEndian.Uint32(header[0:4])
		ksiz := binary.LittleEndian.Uint32(header[4:8])
		vsiz := binary.LittleEndian.Uint32(header[8:12])

		data := make([]byte, ksiz+vsiz)
		if _, err := io.ReadFull(w.file, data); err != nil {
			return err
		}

		// check sum
		actualSum := crc32.ChecksumIEEE(header[4:])
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
