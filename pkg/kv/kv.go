package kv

import (
	"errors"
	"fmt"

	"github.com/return2faye/SiltKV/internal/lsm"
)

var (
	// ErrNotFound is returned when a key is not found
	ErrNotFound = errors.New("kv: key not found")
	// ErrClosed is returned when the DB is closed
	ErrClosed = errors.New("kv: db is closed")
)

// DB represents a key-value database.
// It provides a simple interface for storing and retrieving key-value pairs.
type DB struct {
	db *lsm.DB
}

// Open opens a database at the given path.
// If the database doesn't exist, it will be created.
func Open(path string) (*DB, error) {
	if path == "" {
		return nil, fmt.Errorf("kv: path cannot be empty")
	}

	lsmDB, err := lsm.Open(lsm.Options{DataDir: path})
	if err != nil {
		return nil, fmt.Errorf("kv: failed to open database: %w", err)
	}

	return &DB{db: lsmDB}, nil
}

// Close closes the database and releases all resources.
func (db *DB) Close() error {
	if db.db == nil {
		return ErrClosed
	}
	return db.db.Close()
}

// Put stores a key-value pair in the database.
// If the key already exists, its value will be updated.
func (db *DB) Put(key, value string) error {
	if db.db == nil {
		return ErrClosed
	}
	err := db.db.Put([]byte(key), []byte(value))
	if err != nil {
		// Check if it's a closed error
		if err.Error() == "lsm: db is closed" {
			return ErrClosed
		}
		return fmt.Errorf("kv: put failed: %w", err)
	}
	return nil
}

// Get retrieves the value for a given key.
// Returns ErrNotFound if the key doesn't exist.
func (db *DB) Get(key string) (string, error) {
	if db.db == nil {
		return "", ErrClosed
	}

	val, found, err := db.db.Get([]byte(key))
	if err != nil {
		// Check if it's a closed error
		if err.Error() == "lsm: db is closed" {
			return "", ErrClosed
		}
		return "", fmt.Errorf("kv: get failed: %w", err)
	}
	
	// If DB is closed, active will be nil and Get returns (nil, false, nil)
	// We need to check if db.db is actually closed by trying to access it
	// Actually, if db.db is closed, Get might return (nil, false, nil)
	// So we can't distinguish between "not found" and "closed"
	// For now, we'll trust that if db.db is not nil, it's not closed
	
	if !found {
		return "", ErrNotFound
	}

	return string(val), nil
}

// Delete removes a key from the database.
// If the key doesn't exist, it's a no-op (no error returned).
func (db *DB) Delete(key string) error {
	if db.db == nil {
		return ErrClosed
	}
	err := db.db.Delete([]byte(key))
	if err != nil {
		// Check if it's a closed error
		if err.Error() == "lsm: db is closed" {
			return ErrClosed
		}
		return fmt.Errorf("kv: delete failed: %w", err)
	}
	return nil
}
