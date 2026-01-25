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

	// compaction coordination
	compactWg      sync.WaitGroup
	compactTrigger int // number of SSTables before triggering compaction
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
		dataDir:        opts.DataDir,
		active:         mt,
		sstables:       sstables,
		compactTrigger: 4,
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

	// Check if compaction is needed after adding new SSTable
	shouldCompact := len(db.sstables) >= db.compactTrigger
	db.mu.Unlock()

	// Update manifest (outside lock, I/O operation)
	if err := appendToManifest(db.dataDir, sstPath); err != nil {
		// TODO: log error (for now, just continue)
		// In production, you might want to handle this better
	}

	// Close memtable (this closes WAL)
	mt.Close()

	// Trigger compaction if needed (outside lock to avoid deadlock)
	if shouldCompact {
		db.compactWg.Add(1)
		go db.compactSSTables()
	}

	// TODO: Optionally remove old WAL file (os.Remove(walPath))
	// For now, we'll leave it for debugging
}

// compactSSTables merges multiple SSTables into one.
// It's called when the number of SSTables exceeds the threshold.
// Only the oldest N SSTables are compacted (newest SSTables are preserved).
func (db *DB) compactSSTables() {
	defer db.compactWg.Done()

	// Get SSTables to compact (hold lock briefly)
	db.mu.Lock()
	if len(db.sstables) < db.compactTrigger {
		db.mu.Unlock()
		return
	}

	// Select only the oldest N SSTables to compact (from the end of the list)
	// Newest SSTables are preserved to avoid merging them immediately
	compactCount := db.compactTrigger
	if len(db.sstables) < compactCount {
		compactCount = len(db.sstables)
	}

	// Get the oldest N SSTables (from the end, since list is newest-first)
	startIdx := len(db.sstables) - compactCount
	readersToCompact := make([]*sstable.Reader, compactCount)
	copy(readersToCompact, db.sstables[startIdx:])

	// Track old paths for cleanup
	oldPaths := make([]string, len(readersToCompact))
	for i, r := range readersToCompact {
		oldPaths[i] = r.Path()
	}

	db.mu.Unlock()

	if len(readersToCompact) == 0 {
		return
	}

	// Create merge iterator
	mergeIt, err := sstable.NewMergeIterator(readersToCompact)
	if err != nil {
		// TODO: log error
		return
	}

	// Write merged data, splitting into multiple SSTables if needed
	var newReaders []*sstable.Reader
	var outputPaths []string
	fileCounter := 0
	baseTimestamp := time.Now().UnixNano()

	// Create first writer
	outputPath := filepath.Join(db.dataDir, fmt.Sprintf("compact-%d-%d.sst", baseTimestamp, fileCounter))
	writer, err := sstable.NewWriter(outputPath)
	if err != nil {
		// TODO: log error
		return
	}
	outputPaths = append(outputPaths, outputPath)

	// Write merged data (keep tombstones for now)
	written := 0
	for mergeIt.Valid() {
		key := mergeIt.Key()
		value := mergeIt.Value()

		// Check if current file would exceed size limit
		recordSize := int64(8 + len(key) + len(value))
		if writer.Size()+recordSize > sstable.MaxSSTableFileSize() && writer.Size() > 0 {
			// Close current writer and create new one
			if err := writer.Close(); err != nil {
				// Cleanup on error
				for _, p := range outputPaths {
					os.Remove(p)
				}
				// TODO: log error
				return
			}

			// Open reader for completed file
			reader, err := sstable.NewReader(outputPath)
			if err != nil {
				// Cleanup on error
				for _, p := range outputPaths {
					os.Remove(p)
				}
				// TODO: log error
				return
			}
			newReaders = append(newReaders, reader)

			// Create new writer
			fileCounter++
			outputPath = filepath.Join(db.dataDir, fmt.Sprintf("compact-%d-%d.sst", baseTimestamp, fileCounter))
			writer, err = sstable.NewWriter(outputPath)
			if err != nil {
				// Cleanup on error
				for _, r := range newReaders {
					r.Close()
				}
				for _, p := range outputPaths {
					os.Remove(p)
				}
				// TODO: log error
				return
			}
			outputPaths = append(outputPaths, outputPath)
		}

		// Write key-value pair (including tombstones)
		// Tombstones (nil values) are kept to mark deletions
		_, err := writer.Write(key, value)
		if err != nil {
			writer.Close()
			for _, r := range newReaders {
				r.Close()
			}
			for _, p := range outputPaths {
				os.Remove(p)
			}
			// TODO: log error
			return
		}
		written++

		if err := mergeIt.Next(); err != nil {
			break
		}
	}

	// Close last writer
	if err := writer.Close(); err != nil {
		for _, r := range newReaders {
			r.Close()
		}
		for _, p := range outputPaths {
			os.Remove(p)
		}
		// TODO: log error
		return
	}

	// Open reader for last file
	lastReader, err := sstable.NewReader(outputPath)
	if err != nil {
		for _, r := range newReaders {
			r.Close()
		}
		for _, p := range outputPaths {
			os.Remove(p)
		}
		// TODO: log error
		return
	}
	newReaders = append(newReaders, lastReader)

	// Replace old SSTables with new one
	db.mu.Lock()
	// Check if sstables list has changed significantly (another compaction might have happened)
	// We check if the old SSTables we're trying to replace still exist at the end
	if len(db.sstables) < len(readersToCompact) {
		// SSTables were removed by another compaction, abort
		for _, r := range newReaders {
			r.Close()
		}
		for _, r := range readersToCompact {
			r.Close()
		}
		db.mu.Unlock()
		for _, p := range outputPaths {
			os.Remove(p)
		}
		return
	}

	// Verify the SSTables we're replacing are still at the end
	// (they should be the oldest ones)
	// Recalculate startIdx in case sstables list changed
	currentStartIdx := len(db.sstables) - len(readersToCompact)
	stillMatch := true
	for i, r := range readersToCompact {
		if currentStartIdx+i >= len(db.sstables) || db.sstables[currentStartIdx+i] != r {
			stillMatch = false
			break
		}
	}

	if !stillMatch {
		// SSTables were changed, abort
		for _, r := range newReaders {
			r.Close()
		}
		for _, r := range readersToCompact {
			r.Close()
		}
		db.mu.Unlock()
		for _, p := range outputPaths {
			os.Remove(p)
		}
		return
	}

	// Close old readers
	for _, r := range readersToCompact {
		r.Close()
	}

	// Replace only the compacted SSTables with new ones
	// Merged SSTables should be placed at the position of the old SSTables they replaced
	// (not at the front, because they contain old data, not new data)
	db.sstables = append(
		db.sstables[:currentStartIdx], // Keep newer SSTables at the front
		newReaders...,                 // Place merged SSTables where old ones were
	)

	// Get all current SSTable paths for manifest rewrite
	currentPaths := make([]string, len(db.sstables))
	for i, r := range db.sstables {
		currentPaths[i] = r.Path()
	}

	// Check if we need to trigger another compaction
	shouldCompactAgain := len(db.sstables) >= db.compactTrigger
	db.mu.Unlock()

	// Delete old SSTable files (outside lock)
	for _, path := range oldPaths {
		if err := os.Remove(path); err != nil {
			// TODO: log error (file might already be deleted)
		}
	}

	// Rewrite manifest with current SSTable list
	if err := rewriteManifest(db.dataDir, currentPaths); err != nil {
		// TODO: log error
		// Manifest update failed, but compaction succeeded
		// Next Open will rebuild manifest from disk
	}

	// Trigger another compaction if needed (outside lock to avoid deadlock)
	if shouldCompactAgain {
		db.compactWg.Add(1)
		go db.compactSSTables()
	}
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
