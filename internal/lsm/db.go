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

var (
	ErrClosed     = errors.New("lsm: db is closed")
	ErrEmptyValue = errors.New("lsm: empty values are reserved for tombstones")
)

type DB struct {
	mu            sync.RWMutex
	closed        bool
	backgroundErr error

	active    *memtable.Memtable
	immutable *memtable.Memtable

	// sstable should be read-only for DB user
	sstables []*sstable.Reader

	dataDir string

	// flush coordination
	flushWg sync.WaitGroup // wait for flush goroutines to finish

	// compaction coordination
	compactWg      sync.WaitGroup
	compactMu      sync.Mutex
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
			for _, opened := range sstables {
				opened.Close()
			}
			return nil, fmt.Errorf("open SSTable %q: %w", sstPaths[i], err)
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
			if err := db.getBackgroundError(); err != nil {
				_ = db.Close()
				return nil, err
			}
		}
	}

	return db, nil
}

func (db *DB) setBackgroundError(err error) {
	if err == nil {
		return
	}
	db.mu.Lock()
	if db.backgroundErr == nil {
		db.backgroundErr = err
	}
	db.mu.Unlock()
}

func (db *DB) getBackgroundError() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.backgroundErr
}

// flushMemtable flushes an immutable memtable to disk as an SSTable.
// This runs in a background goroutine.
func (db *DB) flushMemtable(mt *memtable.Memtable, walPath string) {
	defer db.flushWg.Done()

	// Generate SSTable file path
	sstPath := walPath[:len(walPath)-4] + ".sst" // replace .wal with .sst
	fail := func(err error) {
		db.setBackgroundError(fmt.Errorf("flush %q: %w", filepath.Base(walPath), err))
	}
	discardSSTable := func() {
		_ = os.Remove(sstPath)
	}

	// Create writer and flush
	writer, err := sstable.NewWriter(sstPath)
	if err != nil {
		fail(err)
		return
	}

	it := mt.NewIterator()
	if err := writer.WriteFromIterator(it); err != nil {
		_ = writer.Close()
		discardSSTable()
		fail(err)
		return
	}

	if err := writer.Close(); err != nil {
		discardSSTable()
		fail(err)
		return
	}
	if err := syncDir(db.dataDir); err != nil {
		discardSSTable()
		fail(err)
		return
	}

	// Open reader for the new SSTable
	reader, err := sstable.NewReader(sstPath)
	if err != nil {
		discardSSTable()
		fail(err)
		return
	}

	if err := appendToManifest(db.dataDir, sstPath); err != nil {
		_ = reader.Close()
		// append may have reached disk before fsync failed, so retain both the
		// referenced SSTable and WAL for safe recovery.
		fail(err)
		return // keep the immutable memtable and WAL recoverable
	}

	// Register only after the durable manifest references the durable SSTable.
	db.mu.Lock()
	db.sstables = append([]*sstable.Reader{reader}, db.sstables...)
	if db.immutable == mt {
		db.immutable = nil
	}
	shouldCompact := !db.closed && len(db.sstables) >= db.compactTrigger
	db.mu.Unlock()

	// Close memtable (this closes WAL)
	if err := mt.Close(); err != nil {
		fail(err)
		return
	}

	// Delete old WAL file after successful flush
	// The data is now safely persisted in SSTable, so the WAL is no longer needed.
	// This prevents WAL files from accumulating on disk.
	if err := os.Remove(walPath); err != nil {
		// The manifest already references the SSTable; a stale WAL is recoverable.
	} else {
		_ = syncDir(db.dataDir)
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
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

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
		db.setBackgroundError(fmt.Errorf("compaction: %w", err))
		return
	}

	// Write merged data, splitting into multiple SSTables if needed
	var newReaders []*sstable.Reader
	var outputPaths []string
	var writer *sstable.Writer
	cleanup := func() {
		if writer != nil {
			_ = writer.Close()
			writer = nil
		}
		for _, r := range newReaders {
			_ = r.Close()
		}
		for _, p := range outputPaths {
			_ = os.Remove(p)
		}
	}
	fail := func(err error) {
		cleanup()
		db.setBackgroundError(fmt.Errorf("compaction: %w", err))
	}
	fileCounter := 0
	baseTimestamp := time.Now().UnixNano()

	// Create first writer
	outputPath := filepath.Join(db.dataDir, fmt.Sprintf("compact-%d-%d.sst", baseTimestamp, fileCounter))
	writer, err = sstable.NewWriter(outputPath)
	if err != nil {
		fail(err)
		return
	}
	outputPaths = append(outputPaths, outputPath)

	// Write merged data
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
					writer = nil
					fail(err)
					return
				}
				writer = nil

				// Open reader for completed file
				reader, err := sstable.NewReader(outputPath)
				if err != nil {
					fail(err)
					return
				}
				newReaders = append(newReaders, reader)

				// Create new writer
				fileCounter++
				outputPath = filepath.Join(db.dataDir, fmt.Sprintf("compact-%d-%d.sst", baseTimestamp, fileCounter))
				writer, err = sstable.NewWriter(outputPath)
				if err != nil {
					fail(err)
					return
				}
				outputPaths = append(outputPaths, outputPath)
			}

			// Write key-value pair (non-tombstone)
			if _, err := writer.Write(key, value); err != nil {
				fail(err)
				return
			}
		}

		if err := mergeIt.Next(); err != nil {
			fail(err)
			return
		}
	}

	// Close last writer
	if err := writer.Close(); err != nil {
		writer = nil
		fail(err)
		return
	}
	writer = nil

	// Open reader for last file
	lastReader, err := sstable.NewReader(outputPath)
	if err != nil {
		fail(err)
		return
	}
	newReaders = append(newReaders, lastReader)
	if err := syncDir(db.dataDir); err != nil {
		fail(err)
		return
	}

	// Publish the new set in the manifest before removing any old file.
	db.mu.Lock()
	// Check if sstables list has changed significantly (another compaction might have happened)
	// We check if the old SSTables we're trying to replace still exist at the end
	if len(db.sstables) < len(readersToCompact) {
		db.mu.Unlock()
		cleanup()
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
		db.mu.Unlock()
		cleanup()
		return
	}

	replacement := append([]*sstable.Reader(nil), db.sstables[:currentStartIdx]...)
	replacement = append(replacement, newReaders...)
	// The manifest is oldest-first; the in-memory slice is newest-first.
	manifestPaths := make([]string, len(replacement))
	for i, r := range replacement {
		manifestPaths[len(replacement)-1-i] = r.Path()
	}
	if err := rewriteManifest(db.dataDir, manifestPaths); err != nil {
		db.mu.Unlock()
		fail(err)
		return
	}

	db.sstables = replacement
	for _, r := range readersToCompact {
		r.Close()
	}
	// ponytail: count-based compaction stops if splitting cannot reduce file
	// count; add levels only when sustained large datasets prove this ceiling.
	shouldCompactAgain := !db.closed && len(newReaders) < len(readersToCompact) && len(db.sstables) >= db.compactTrigger
	db.mu.Unlock()

	for _, path := range oldPaths {
		_ = os.Remove(path)
	}
	_ = syncDir(db.dataDir)

	// Trigger another compaction if needed (outside lock to avoid deadlock)
	if shouldCompactAgain {
		db.compactWg.Add(1)
		go db.compactSSTables()
	}
}

func (db *DB) Close() error {
	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.mu.Unlock()

	// A flush can start compaction, so drain them in that order before closing
	// any reader or WAL they may still use.
	db.flushWg.Wait()
	db.compactWg.Wait()

	db.mu.Lock()
	active := db.active
	immutable := db.immutable
	sstables := db.sstables
	firstErr := db.backgroundErr

	// Mark as closed
	db.active = nil
	db.immutable = nil
	db.sstables = nil
	db.mu.Unlock()

	// close resource outside of lock
	// avoid holding lock during I/O

	if active != nil {
		if err := active.Close(); err != nil && firstErr == nil {
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

	return firstErr
}

// Put writes a key-value pair to the WAL-backed active memtable and rotates it
// when it reaches the configured size limit.
func (db *DB) Put(key, value []byte) error {
	if value != nil && len(value) == 0 {
		return ErrEmptyValue
	}
	for {
		db.mu.RLock()
		if db.closed {
			db.mu.RUnlock()
			return ErrClosed
		}
		if db.backgroundErr != nil {
			err := db.backgroundErr
			db.mu.RUnlock()
			return err
		}
		mt := db.active
		db.mu.RUnlock()

		if mt == nil {
			return ErrClosed
		}

		if err := mt.Put(key, value); err != nil {
			if errors.Is(err, memtable.ErrFrozen) {
				continue // rotation raced us; retry on the new active table
			}
			return err
		}

		if mt.IsFull() {
			return db.rotateMemtable(mt)
		}

		return nil
	}
}

// rotateMemtable freezes the current active, moves it to immutable,
// creates a new active, and starts a background flush.
func (db *DB) rotateMemtable(expected *memtable.Memtable) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.backgroundErr != nil {
		return db.backgroundErr
	}
	if db.active != expected {
		return nil
	}

	// Check if already rotating (immutable exists)
	if db.immutable != nil {
		// Previous flush not finished yet, just return
		// In production, you might want to wait or return an error
		return nil
	}

	// Create the replacement first so a filesystem error cannot leave the
	// current active memtable frozen with nowhere for writes to go.
	newWalPath := filepath.Join(db.dataDir, fmt.Sprintf("active-%d.wal", time.Now().UnixNano()))
	newActive, err := memtable.NewMemtable(newWalPath)
	if err != nil {
		return err
	}

	if err := db.active.Freeze(); err != nil {
		_ = newActive.Close()
		_ = os.Remove(newWalPath)
		db.backgroundErr = fmt.Errorf("freeze WAL: %w", err)
		return db.backgroundErr
	}

	oldWalPath := db.active.WalPath()
	db.immutable = db.active
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
	defer db.mu.RUnlock()
	if db.closed {
		return nil, false, ErrClosed
	}
	active := db.active
	immutable := db.immutable
	sstables := db.sstables

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
			return nil, false, err
		}
		if found {
			if val == nil {
				return nil, false, nil
			}
			return val, true, nil
		}
		// SSTable time ranges overlap, so a miss must continue to older files.
	}

	return nil, false, nil
}

func (db *DB) Delete(key []byte) error {
	return db.Put(key, nil)
}
