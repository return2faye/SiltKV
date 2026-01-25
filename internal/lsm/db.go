package lsm

import (
	"errors"
	"os"
	"path/filepath"
	"sync"

	"github.com/macz/SiltKV/internal/memtable"
	"github.com/macz/SiltKV/internal/sstable"
	"github.com/macz/SiltKV/internal/utils"
)

var ErrClosed = errors.New("lsm: db is closed")

type DB struct {
	mu sync.RWMutex

	active *memtable.Memtable
	immutable *memtable.Memtable

	// sstable should be read-only for DB user
	sstables []*sstable.Reader

	dataDir string
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

	walPath := filepath.Join(opts.DataDir, "active.wal")
	mt, err := memtable.NewMemtable(walPath)
	if err != nil {
		return nil, err
	}

	db := &DB{
		dataDir: opts.DataDir,
		active: mt,
		// immutable: nil,
		// sstables: nil,
	}

	return db, nil
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

	return mt.Put(key, value)
}

func (db *DB) Get(key []byte) ([]byte, bool, error) {
	db.mu.RLock()
	active := db.active
	db.mu.RUnlock()

	if active == nil {
		return nil, false, ErrClosed
	}

	val, found := active.Get(key)
	if !found {
		return nil, false, nil
	}

	if val != nil {
		cp := utils.CopyBytes(val)
		return cp, true, nil
	}

	return nil, false, nil
}

func (db *DB) Delete(key []byte) error {
	return db.Put(key, nil)
}