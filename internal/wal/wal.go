package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"
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
	// maxKeySize is the maximum allowed key size (128B, tuned for web workloads)
	maxKeySize = 128
	// maxValueSize is the maximum allowed value size (4KB, compressed JSON payload)
	maxValueSize = 4 * 1024
	// maxRecordSize is the maximum allowed total record size (header + key + value)
	maxRecordSize = headerSize + maxKeySize + maxValueSize
	// maxWriteBufSize is the maximum buffer size before forcing a flush (64KB)
	maxWriteBufSize = 64 << 10
)

// Write-Ahead Log implementation
type WalWriter struct {
	mu        sync.Mutex
	file      *os.File
	buf       []byte // reusable buffer for encoding a single record
	headerBuf []byte // reusable buffer for Load header (fixed 12 bytes)
	dataBuf   []byte // reusable buffer for Load data (grows as needed)

	// Buffered writes for better throughput
	writeBuf   []byte // buffer for batched writes
	bufSize    int    // current buffer size
	maxBufSize int    // maximum buffer size before flush

	closed   bool
	asyncErr error // background fsync error (surfaced on Write/Sync)

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewWalWriter(path string) (*WalWriter, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	w := &WalWriter{
		file:       f,
		buf:        make([]byte, 0, initialBufferSize),     // pre-allocate write buffer capacity
		headerBuf:  make([]byte, headerSize),               // fixed-size header buffer
		dataBuf:    make([]byte, 0, initialDataBufferSize), // pre-allocate data buffer capacity
		writeBuf:   make([]byte, 0, maxWriteBufSize),       // pre-allocate write buffer
		maxBufSize: maxWriteBufSize,
		stopCh:     make(chan struct{}),
	}

	// Start background fsync loop (time-driven durability)
	w.wg.Add(1)
	go w.syncLoop(time.Second)

	return w, nil
}

func (w *WalWriter) Write(key, value []byte) error {
	ksiz := len(key)
	vsiz := len(value)

	// Fail Fast: Validate sizes before any allocation or I/O
	// This prevents silent data loss (write succeeds but can't be recovered)
	if ksiz > maxKeySize {
		return ErrInvalidSize
	}
	if vsiz > maxValueSize {
		return ErrInvalidSize
	}
	if ksiz+vsiz > maxRecordSize-headerSize {
		return ErrInvalidSize
	}

	neededSize := headerSize + ksiz + vsiz

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed || w.file == nil {
		return ErrClosed
	}
	if w.asyncErr != nil {
		return w.asyncErr
	}

	// Prepare the record in a reusable buffer under lock.
	// This ensures concurrent Write calls don't race on the shared w.buf slice.
	if cap(w.buf) < neededSize {
		w.buf = make([]byte, neededSize)
	}
	buf := w.buf[:neededSize]

	// header: checksum(4) | kSize(4) | vSize(4)
	binary.LittleEndian.PutUint32(buf[4:8], uint32(ksiz))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(vsiz))

	copy(buf[12:], key)
	copy(buf[12+ksiz:], value)

	sum := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], sum)

	// Append encoded record to write buffer
	w.writeBuf = append(w.writeBuf, buf...)
	w.bufSize += neededSize

	// Flush to OS page cache if buffer is large enough
	if w.bufSize >= w.maxBufSize {
		if err := w.flushBufferLocked(); err != nil {
			return err
		}
	}

	return nil
}

// flushBufferLocked flushes the write buffer to OS page cache.
// Must be called with mu held.
func (w *WalWriter) flushBufferLocked() error {
	if len(w.writeBuf) == 0 {
		return nil
	}

	_, err := w.file.Write(w.writeBuf)
	if err != nil {
		return err
	}

	// Reset buffer
	w.writeBuf = w.writeBuf[:0]
	w.bufSize = 0
	return nil
}

// file.Write only writes to Page Cache in Kernel
// fsync forces swap data in cache into disk
func (w *WalWriter) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed || w.file == nil {
		return ErrClosed
	}
	if w.asyncErr != nil {
		return w.asyncErr
	}

	// Flush any pending buffered writes first
	if err := w.flushBufferLocked(); err != nil {
		return err
	}

	// Explicit Sync is allowed to block and provides strong durability.
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
	if w.closed {
		w.mu.Unlock()
		w.wg.Wait()
		return nil
	}
	w.closed = true
	// Stop background sync loop
	close(w.stopCh)
	w.mu.Unlock()

	// Wait for background goroutine to exit
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.file == nil {
		return nil
	}

	// Flush pending writes then fsync and close.
	flushErr := w.flushBufferLocked()
	syncErr := w.file.Sync()
	closeErr := w.file.Close()
	w.file = nil

	if flushErr != nil {
		return flushErr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

// syncLoop periodically flushes buffered writes and fsyncs the WAL.
// It is best-effort: any error is recorded and returned by future Write/Sync calls.
func (w *WalWriter) syncLoop(interval time.Duration) {
	defer w.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			if w.closed || w.file == nil {
				w.mu.Unlock()
				return
			}
			if w.asyncErr != nil {
				w.mu.Unlock()
				continue
			}

			// Ensure data reaches OS page cache before fsync.
			if err := w.flushBufferLocked(); err != nil {
				w.asyncErr = err
				w.mu.Unlock()
				continue
			}
			f := w.file
			w.mu.Unlock()

			if err := f.Sync(); err != nil {
				w.mu.Lock()
				if w.asyncErr == nil {
					w.asyncErr = err
				}
				w.mu.Unlock()
			}
		case <-w.stopCh:
			return
		}
	}
}
