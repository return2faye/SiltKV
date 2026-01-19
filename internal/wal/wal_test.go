package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWriteAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	// Create WAL writer
	wal, err := NewWalWriter(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL writer: %v", err)
	}
	defer wal.Close()

	// Write some data in deterministic order
	testData := []struct {
		key   string
		value []byte
	}{
		{"key1", []byte("value1")},
		{"key2", []byte("value2")},
		{"key3", []byte("value3")},
	}

	expectedData := make(map[string][]byte)
	for _, d := range testData {
		expectedData[d.key] = d.value
		if err := wal.Write([]byte(d.key), d.value); err != nil {
			t.Fatalf("Failed to write %s: %v", d.key, err)
		}
	}

	// Sync to ensure data is written
	if err := wal.Sync(); err != nil {
		t.Fatalf("Failed to sync: %v", err)
	}

	// Close and reopen
	wal.Close()

	wal2, err := NewWalWriter(walPath)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	// Load and verify
	loaded := make(map[string][]byte)
	result, err := wal2.Load(func(k, v []byte) {
		// WAL loads in write order, so if same key appears multiple times,
		// last write wins (which is correct behavior)
		// Deep copy value to avoid referencing internal buffer
		var valCopy []byte
		if v != nil {
			valCopy = make([]byte, len(v))
			copy(valCopy, v)
		}
		loaded[string(k)] = valCopy
	})

	if err != nil {
		t.Fatalf("Failed to load: %v", err)
	}

	if result.Recovered != len(testData) {
		t.Errorf("Expected %d records, got %d", len(testData), result.Recovered)
	}

	if result.Skipped != 0 {
		t.Errorf("Expected 0 skipped records, got %d", result.Skipped)
	}

	// Verify all data
	// Since we write each key once, all should be present with correct values
	if len(loaded) != len(expectedData) {
		t.Errorf("Expected %d keys loaded, got %d", len(expectedData), len(loaded))
	}

	// Verify each key individually (avoid map iteration order issues)
	for k, expectedV := range expectedData {
		actualV, ok := loaded[k]
		if !ok {
			t.Errorf("Key %s not found in loaded data", k)
			continue
		}
		if string(actualV) != string(expectedV) {
			t.Errorf("Key %s: expected %s, got %s", k, string(expectedV), string(actualV))
		}
	}
}

func TestTombstone(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := NewWalWriter(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL writer: %v", err)
	}
	defer wal.Close()

	// Write a key
	if err := wal.Write([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to write: %v", err)
	}

	// Delete it (tombstone)
	if err := wal.Write([]byte("key1"), nil); err != nil {
		t.Fatalf("Failed to write tombstone: %v", err)
	}

	wal.Close()

	// Reload
	wal2, err := NewWalWriter(walPath)
	if err != nil {
		t.Fatalf("Failed to reopen WAL: %v", err)
	}
	defer wal2.Close()

	// Load and verify tombstone is handled
	var lastValue []byte
	result, err := wal2.Load(func(k, v []byte) {
		if string(k) == "key1" {
			lastValue = v
		}
	})

	if err != nil {
		t.Fatalf("Failed to load: %v", err)
	}

	if result.Recovered != 2 {
		t.Errorf("Expected 2 records (write + delete), got %d", result.Recovered)
	}

	// Last operation should be delete (nil value)
	if lastValue != nil {
		t.Error("Expected tombstone (nil value) for deleted key")
	}
}

func TestClose(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	wal, err := NewWalWriter(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL writer: %v", err)
	}

	// Close
	if err := wal.Close(); err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	// Try to write after close
	err = wal.Write([]byte("key"), []byte("value"))
	if err != ErrClosed {
		t.Errorf("Expected ErrClosed, got %v", err)
	}

	// Try to sync after close
	err = wal.Sync()
	if err != ErrClosed {
		t.Errorf("Expected ErrClosed on Sync, got %v", err)
	}

	// Try to load after close
	_, err = wal.Load(func(k, v []byte) {})
	if err != ErrClosed {
		t.Errorf("Expected ErrClosed on Load, got %v", err)
	}

	// Close again should be safe
	if err := wal.Close(); err != nil {
		t.Errorf("Second close should be safe, got error: %v", err)
	}
}

func TestLoadEmptyFile(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "empty.wal")

	// Create empty file
	f, err := os.Create(walPath)
	if err != nil {
		t.Fatalf("Failed to create empty file: %v", err)
	}
	f.Close()

	wal, err := NewWalWriter(walPath)
	if err != nil {
		t.Fatalf("Failed to open empty WAL: %v", err)
	}
	defer wal.Close()

	result, err := wal.Load(func(k, v []byte) {
		t.Error("Load callback should not be called for empty file")
	})

	if err != nil {
		t.Fatalf("Load should succeed on empty file, got: %v", err)
	}

	if result.Recovered != 0 {
		t.Errorf("Expected 0 recovered records, got %d", result.Recovered)
	}
}
