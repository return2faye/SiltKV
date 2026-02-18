package lsm

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/return2faye/SiltKV/internal/memtable"
	"github.com/return2faye/SiltKV/internal/sstable"
	"github.com/return2faye/SiltKV/internal/utils"
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

type walSegment struct {
	path string
	ts   int64
}

func listWALSegments(dataDir string) ([]walSegment, error) {
	matches, err := filepath.Glob(filepath.Join(dataDir, "*.wal"))
	if err != nil {
		return nil, err
	}

	segs := make([]walSegment, 0, len(matches))
	for _, p := range matches {
		base := filepath.Base(p)

		// Our WAL naming scheme:
		// - "active.wal" (initial)
		// - "active-<unixNano>.wal" (after rotations)
		var ts int64
		switch {
		case base == "active.wal":
			ts = 0
		case strings.HasPrefix(base, "active-") && strings.HasSuffix(base, ".wal"):
			num := strings.TrimSuffix(strings.TrimPrefix(base, "active-"), ".wal")
			if v, err := strconv.ParseInt(num, 10, 64); err == nil {
				ts = v
			} else {
				// Fallback to file modtime if name can't be parsed.
				if st, statErr := os.Stat(p); statErr == nil {
					ts = st.ModTime().UnixNano()
				}
			}
		default:
			// Unknown WAL name; still recover it. Use modtime ordering.
			if st, statErr := os.Stat(p); statErr == nil {
				ts = st.ModTime().UnixNano()
			}
		}

		segs = append(segs, walSegment{path: p, ts: ts})
	}

	sort.Slice(segs, func(i, j int) bool {
		if segs[i].ts != segs[j].ts {
			return segs[i].ts < segs[j].ts
		}
		return segs[i].path < segs[j].path
	})

	return segs, nil
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

	// Discover WAL segments (crash during rotation may leave multiple WAL files).
	segs, err := listWALSegments(opts.DataDir)
	if err != nil {
		return nil, err
	}

	// If no WAL exists, create the default active WAL.
	if len(segs) == 0 {
		segs = append(segs, walSegment{path: filepath.Join(opts.DataDir, "active.wal"), ts: 0})
	}

	// The newest WAL segment becomes the active memtable.
	activeWalPath := segs[len(segs)-1].path
	mt, err := memtable.NewMemtable(activeWalPath)
	if err != nil {
		return nil, err
	}

	db := &DB{
		dataDir:        opts.DataDir,
		active:         mt,
		sstables:       sstables,
		compactTrigger: 4,
	}

	// Any older WAL segments represent data that was not flushed to SSTables yet.
	// To keep the runtime model simple (active + optional immutable), we flush these
	// older WAL segments to SSTables during Open and delete them after a successful flush.
	//
	// Recovery order matters: old -> new. By flushing older segments first and using the
	// newest as active, we preserve last-write-wins semantics on reads (active checked first).
	if len(segs) > 1 {
		for _, seg := range segs[:len(segs)-1] {
			oldMt, err := memtable.NewMemtable(seg.path)
			if err != nil {
				mt.Close()
				return nil, err
			}
			if err := oldMt.Freeze(); err != nil {
				oldMt.Close()
				mt.Close()
				return nil, err
			}

			// Flush synchronously during Open to avoid leaving background work
			// tied to a DB that might be immediately closed by the caller.
			db.flushWg.Add(1)
			db.flushMemtable(oldMt, seg.path)
		}
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

	// Delete old WAL file after successful flush
	// The data is now safely persisted in SSTable, so the WAL is no longer needed.
	// This prevents WAL files from accumulating on disk.
	if err := os.Remove(walPath); err != nil {
		// Log warning but don't fail (WAL deletion is not critical for correctness)
		// The SSTable already contains the data, so the system can continue operating
		// TODO: log warning (for now, just continue)
	}

	// Trigger compaction if needed (outside lock to avoid deadlock)
	if shouldCompact {
		db.compactWg.Add(1)
		go db.compactSSTables()
	}
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

	// Write merged data
	written := 0
	for mergeIt.Valid() {
		key := mergeIt.Key()
		value := mergeIt.Value()

		// Skip tombstones: if value is nil, we don't write it to compacted SSTables.
		// This is safe because compactSSTables always operates on the oldest N SSTables,
		// so all older versions of this key are included in this compaction.
		if value != nil {
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

			// Write key-value pair (non-tombstone)
			if _, err := writer.Write(key, value); err != nil {
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
		}

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

	// Save the old WAL path before moving to immutable
	oldWalPath := db.active.WalPath()

	// Move to immutable
	db.immutable = db.active

	// Create new active with new WAL
	newWalPath := filepath.Join(db.dataDir, fmt.Sprintf("active-%d.wal", time.Now().UnixNano()))
	newActive, err := memtable.NewMemtable(newWalPath)
	if err != nil {
		// Rollback: unfreeze immutable and restore as active
		// For simplicity, we'll just return error (in production, handle better)
		return err
	}

	db.active = newActive

	// Start background flush with the old WAL path (the one that should be deleted)
	db.flushWg.Add(1)
	go db.flushMemtable(db.immutable, oldWalPath)

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
