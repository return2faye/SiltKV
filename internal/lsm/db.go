package lsm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/macz/SiltKV/internal/memtable"
	"github.com/macz/SiltKV/internal/sstable"
	"github.com/macz/SiltKV/internal/utils"
)

var ErrClosed = errors.New("lsm: db is closed")

type DB struct {
	mu sync.RWMutex

	active    *memtable.Memtable
	immutable *memtable.Memtable

	// sstable should be read-only for DB user
	sstables []*sstable.Reader

	dataDir string

	// flush coordination
	flushWg sync.WaitGroup // wait for flush goroutines to finish
}

type Options struct {
	DataDir string
}

func Open(opts Options) (*DB, error) {
	if opts.DataDir == "" {
		return nil, os.ErrInvalid
	}

	if err := os.MkdirAll(opts.DataDir, 0o755); err != nil {
		return nil, err
	}

	// Load existing SSTables from manifest
	sstPaths, err := loadManifest(opts.DataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to load manifest: %w", err)
	}

	// Open all SSTable readers (reverse order: newest first)
	var sstables []*sstable.Reader
	for i := len(sstPaths) - 1; i >= 0; i-- {
		reader, err := sstable.NewReader(sstPaths[i])
		if err != nil {
			// Log error but continue (SSTable might be corrupted or deleted)
			// In production, you might want to handle this better
			continue
		}
		sstables = append(sstables, reader)
	}

	walPath := filepath.Join(opts.DataDir, "active.wal")
	mt, err := memtable.NewMemtable(walPath)
	if err != nil {
		return nil, err
	}

	db := &DB{
		dataDir: opts.DataDir,
		active:  mt,
		// immutable: nil,
		// sstables: nil,
	}

	return db, nil
}

// flushMemtable flushes an immutable memtable to disk as an SSTable.
// This runs in a background goroutine.
func (db *DB) flushMemtable(mt *memtable.Memtable, walPath string) {
	defer db.flushWg.Done()

	// Generate SSTable file path
	sstPath := walPath[:len(walPath)-4] + ".sst" // replace .wal with .sst

	// Create writer and flush
	writer, err := sstable.NewWriter(sstPath)
	if err != nil {
		// TODO: log error (for now, just return)
		return
	}

	it := mt.NewIterator()
	if err := writer.WriteFromIterator(it); err != nil {
		writer.Close()
		// TODO: log error
		return
	}

	if err := writer.Close(); err != nil {
		// TODO: log error
		return
	}

	// Open reader for the new SSTable
	reader, err := sstable.NewReader(sstPath)
	if err != nil {
		// TODO: log error
		return
	}

	// Register SSTable reader (newest first)
	db.mu.Lock()
	db.sstables = append([]*sstable.Reader{reader}, db.sstables...)

	// clear immutable since flushed
	if db.immutable == mt {
		db.immutable = nil
	}
	db.mu.Unlock()

	// Update manifest (outside lock, I/O operation)
	if err := appendToManifest(db.dataDir, sstPath); err != nil {
		// TODO: log error (for now, just continue)
		// In production, you might want to handle this better
	}

	// Close memtable (this closes WAL)
	mt.Close()

	// TODO: Optionally remove old WAL file (os.Remove(walPath))
	// For now, we'll leave it for debugging
}

func (db *DB) Close() error {
	db.mu.Lock()
	// No data
	if db.active == nil && db.immutable == nil && len(db.sstables) == 0 {
		return nil
	}

	// Capture references before marking as closed
	active := db.active
	immutable := db.immutable
	sstables := db.sstables

	// Mark as closed
	db.active = nil
	db.immutable = nil
	db.sstables = nil
	db.mu.Unlock()

	// close resource outside of lock
	// avoid holding lock during I/O

	var firstErr error

	if active != nil {
		if err := active.Close(); err != nil {
			firstErr = err
		}
	}
	if immutable != nil {
		if err := immutable.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	for _, r := range sstables {
		if r != nil {
			if err := r.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}

	return nil
}

// Put writes a key-value pair into the DB.
// Currently only writes to the active memtable (no flush/rotation yet).
func (db *DB) Put(key, value []byte) error {
	db.mu.RLock()
	mt := db.active
	db.mu.RUnlock()

	if mt == nil {
		return ErrClosed
	}

	if err := mt.Put(key, value); err != nil {
		return err
	}

	if mt.IsFull() {
		return db.rotateMemtable()
	}

	return nil
}

// rotateMemtable freezes the current active, moves it to immutable,
// creates a new active, and starts a background flush.
func (db *DB) rotateMemtable() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Check if already rotating (immutable exists)
	if db.immutable != nil {
		// Previous flush not finished yet, just return
		// In production, you might want to wait or return an error
		return nil
	}

	// Freeze current active
	db.active.Freeze()

	// Move to immutable
	db.immutable = db.active

	// Create new active with new WAL
	walPath := filepath.Join(db.dataDir, fmt.Sprintf("active-%d.wal", time.Now().UnixNano()))
	newActive, err := memtable.NewMemtable(walPath)
	if err != nil {
		// Rollback: unfreeze immutable and restore as active
		// For simplicity, we'll just return error (in production, handle better)
		return err
	}

	db.active = newActive

	// Start background flush
	db.flushWg.Add(1)
	go db.flushMemtable(db.immutable, walPath)

	return nil
}

// Get reads a key from the DB.
// Lookup order: active memtable → immutable memtable → SSTables (newest first).
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	db.mu.RLock()
	active := db.active
	immutable := db.immutable
	sstables := make([]*sstable.Reader, len(db.sstables))
	copy(sstables, db.sstables) // Copy slice to avoid holding lock
	db.mu.RUnlock()

	// 1. Check active memtable
	if active != nil {
		val, found := active.Get(key)
		if found {
			if val != nil {
				return utils.CopyBytes(val), true, nil
			}
			// Tombstone found in active, return not found
			return nil, false, nil
		}
	}

	// 2. Check immutable memtable
	if immutable != nil {
		val, found := immutable.Get(key)
		if found {
			if val != nil {
				return utils.CopyBytes(val), true, nil
			}
			// Tombstone found in immutable, return not found
			return nil, false, nil
		}
	}

	// 3. Check SSTables (newest first)
	for _, reader := range sstables {
		val, found, err := reader.Get(key)
		if err != nil {
			// Log error but continue to next SSTable
			continue
		}
		if found {
			// Reader.Get already returns a copy, so we can return directly
			return val, true, nil
		}
		// If key > current key in SSTable, we can stop (keys are sorted)
		// But our current Get is linear scan, so we check all SSTables
	}

	return nil, false, nil
}

func (db *DB) Delete(key []byte) error {
	return db.Put(key, nil)
}
