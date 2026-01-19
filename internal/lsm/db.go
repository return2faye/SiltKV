package lsm

import (
	"os"
	"path/filepath"
	"sync"

	"github.com/macz/SiltKV/internal/memtable"
	"github.com/macz/SiltKV/internal/sstable"
)

type DB struct {
	mu sync.RWMutex

	active *memtable.Memtable
	immutable *memtable.Memtable

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
	defer db.mu.Unlock()

	return nil
}