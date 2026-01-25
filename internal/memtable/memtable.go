package memtable

import (
	"errors"
	"log"
	"sync"

	"github.com/return2faye/SiltKV/internal/wal"
)

const (
	// DefaultMaxSize is the default maximum size for memtable (4MB)
	// When memtable reaches this size, it should be flushed to SSTable
	DefaultMaxSize = 4 << 20
)

var ErrFrozen = errors.New("memtable: frozen")

// Memtable wraps SkipList with WAL support for durability
type Memtable struct {
	sl      *SkipList
	wal     *wal.WalWriter
	maxSize int // maximum size before flush
	size    int // current estimated size
	frozen  bool
	mu      sync.RWMutex // protects frozen flag and size counter
}

// NewMemtable creates a new memtable with WAL support
// It automatically recovers data from WAL if the file exists
func NewMemtable(walPath string) (*Memtable, error) {
	// Create WAL writer (opens existing file or creates new one)
	walWriter, err := wal.NewWalWriter(walPath)
	if err != nil {
		return nil, err
	}

	mt := &Memtable{
		sl:      NewSkipList(),
		wal:     walWriter,
		maxSize: DefaultMaxSize,
		size:    0,
	}

	// Recover data from WAL
	if err := mt.recoverFromWAL(); err != nil {
		// If recovery fails, we still return the memtable
		// but it will be empty. The error can be logged.
		// For now, we'll return error to be safe.
		walWriter.Close()
		return nil, err
	}

	return mt, nil
}

// Put inserts or updates a key-value pair
// Writes to WAL first (for durability), then to SkipList (for fast access)
func (mt *Memtable) Put(key, value []byte) error {
	mt.mu.Lock()
	if mt.frozen {
		mt.mu.Unlock()
		return ErrFrozen
	}
	// Step 1: Write to WAL first (persistence)
	// If WAL write fails, we don't write to memory to maintain consistency
	if err := mt.wal.Write(key, value); err != nil {
		mt.mu.Unlock()
		return err
	}

	// Step 2: Force sync to disk (trade-off: safety vs performance)
	if err := mt.wal.Sync(); err != nil {
		mt.mu.Unlock()
		return err
	}

	// Step 3: Write to SkipList (memory)
	// Get old size before update to calculate size change
	oldValue, existed := mt.sl.Get(key)

	mt.sl.Put(key, value)

	// Step 4: Update size estimate
	// Subtract old entry size, add new entry size
	if existed && oldValue != nil {
		mt.size -= len(key) + len(oldValue)
	}
	mt.size += len(key) + len(value)
	mt.mu.Unlock()

	return nil
}

// Get retrieves a value by key from SkipList
// WAL is not queried because it's only for recovery, not for reads
func (mt *Memtable) Get(key []byte) ([]byte, bool) {
	return mt.sl.Get(key)
}

// Delete removes a key by writing a tombstone (value = nil)
// This is written to both WAL and SkipList
func (mt *Memtable) Delete(key []byte) error {
	// Delete is implemented as Put(key, nil)
	// WAL will record value size as 0, which is interpreted as tombstone
	return mt.Put(key, nil)
}

// Size returns the estimated current size of memtable
func (mt *Memtable) Size() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size
}

// IsFull checks if memtable has reached maximum size
// When full, memtable should be flushed to SSTable
func (mt *Memtable) IsFull() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.size >= mt.maxSize
}

// Freeze marks memtable as immutable. Subsequent Put/Delete will fail with ErrFrozen.
// Reads are still allowed. This should be called before flushing to SSTable.
func (mt *Memtable) Freeze() error {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if mt.frozen {
		return nil
	}
	mt.frozen = true
	// Ensure WAL is synced before flush starts
	return mt.wal.Sync()
}

// IsFrozen indicates whether the memtable has been frozen (immutable).
func (mt *Memtable) IsFrozen() bool {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	return mt.frozen
}

// recoverFromWAL restores memtable from WAL file
// This is called automatically during initialization
func (mt *Memtable) recoverFromWAL() error {
	result, err := mt.wal.Load(func(k, v []byte) {
		// For each record in WAL, restore to SkipList
		mt.sl.Put(k, v)

		// Update size estimate
		if v == nil {
			// Tombstone (delete), only count key
			mt.mu.Lock()
			mt.size += len(k)
			mt.mu.Unlock()
		} else {
			mt.mu.Lock()
			mt.size += len(k) + len(v)
			mt.mu.Unlock()
		}
	})

	if err != nil {
		return err
	}

	// Log recovery statistics
	log.Printf("Memtable recovery: %d records recovered, %d skipped",
		result.Recovered, result.Skipped)
	_ = result // Recovery statistics available for logging if needed

	return nil
}

// Close closes the WAL file
// Should be called when memtable is being flushed or destroyed
func (mt *Memtable) Close() error {
	if mt.wal != nil {
		return mt.wal.Close()
	}
	return nil
}

// NewIterator creates an iterator for scanning all entries in memtable
func (mt *Memtable) NewIterator() *SLIterator {
	return mt.sl.NewIterator()
}
