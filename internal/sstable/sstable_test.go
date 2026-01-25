package sstable

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/return2faye/SiltKV/internal/memtable"
)

func TestFlushAndGet(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstPath := filepath.Join(tmpDir, "test.sst")

	// 2. Create memtable and write test data
	mt, err := memtable.NewMemtable(walPath)
	if err != nil {
		t.Fatalf("Failed to create memtable: %v", err)
	}
	defer mt.Close()

	testData := map[string]string{
		"key3": "value3",
		"key1": "value1",
		"key2": "value2",
		"key5": "value5",
		"key4": "value4",
	}

	for k, v := range testData {
		if err := mt.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}

	// 3. Freeze memtable before flushing
	mt.Freeze()

	// 4. Flush memtable into SSTable
	writer, err := NewWriter(sstPath)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	it := mt.NewIterator()
	if err := writer.WriteFromIterator(it); err != nil {
		writer.Close()
		t.Fatalf("Failed to flush: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("Failed to close writer: %v", err)
	}

	// 5. Read back from SSTable and verify
	reader, err := NewReader(sstPath)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Verify that all written keys can be read back
	for k, expectedV := range testData {
		val, found, err := reader.Get([]byte(k))
		if err != nil {
			t.Fatalf("Get error for %s: %v", k, err)
		}
		if !found {
			t.Errorf("Key %s not found", k)
			continue
		}
		if string(val) != expectedV {
			t.Errorf("Key %s: expected %s, got %s", k, expectedV, string(val))
		}
	}

	// Verify that a non-existent key is not found
	_, found, err := reader.Get([]byte("nonexistent"))
	if err != nil {
		t.Fatalf("Get error for nonexistent key: %v", err)
	}
	if found {
		t.Error("Nonexistent key should not be found")
	}
}

func TestIteratorCorruption(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "corrupted.sst")

	// Create a file with invalid header (too short)
	f, err := os.Create(sstPath)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	// Write only 4 bytes instead of 8
	f.Write([]byte{0x01, 0x00, 0x00, 0x00})
	f.Close()

	reader, err := NewReader(sstPath)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	it := reader.NewIterator()
	err = it.Next()
	if err != nil {
		t.Fatalf("Next should handle incomplete header gracefully, got: %v", err)
	}
	if it.Valid() {
		t.Error("Iterator should be invalid after incomplete header")
	}
}

func TestEmptySSTable(t *testing.T) {
	tmpDir := t.TempDir()
	sstPath := filepath.Join(tmpDir, "empty.sst")

	// Create empty file
	f, err := os.Create(sstPath)
	if err != nil {
		t.Fatalf("Failed to create empty file: %v", err)
	}
	f.Close()

	reader, err := NewReader(sstPath)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	it := reader.NewIterator()
	err = it.Next()
	if err != nil {
		t.Fatalf("Next on empty file should succeed, got: %v", err)
	}
	if it.Valid() {
		t.Error("Iterator should be invalid for empty file")
	}

	// Get should return not found
	_, found, err := reader.Get([]byte("anykey"))
	if err != nil {
		t.Fatalf("Get on empty file should succeed, got: %v", err)
	}
	if found {
		t.Error("Get should return not found for empty file")
	}
}

func TestIteratorOrder(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")
	sstPath := filepath.Join(tmpDir, "test.sst")

	mt, err := memtable.NewMemtable(walPath)
	if err != nil {
		t.Fatalf("Failed to create memtable: %v", err)
	}
	defer mt.Close()

	// Put data in random order
	testKeys := []string{"key3", "key1", "key5", "key2", "key4"}
	for _, k := range testKeys {
		if err := mt.Put([]byte(k), []byte("value")); err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}

	mt.Freeze()

	writer, err := NewWriter(sstPath)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	it := mt.NewIterator()
	if err := writer.WriteFromIterator(it); err != nil {
		writer.Close()
		t.Fatalf("Failed to flush: %v", err)
	}
	writer.Close()

	reader, err := NewReader(sstPath)
	if err != nil {
		t.Fatalf("Failed to create reader: %v", err)
	}
	defer reader.Close()

	// Verify iterator order
	sstIt := reader.NewIterator()
	expectedOrder := []string{"key1", "key2", "key3", "key4", "key5"}
	idx := 0

	if err := sstIt.Next(); err != nil {
		t.Fatalf("Failed to move to first record: %v", err)
	}

	for sstIt.Valid() {
		if idx >= len(expectedOrder) {
			t.Errorf("Iterator returned more items than expected")
			break
		}

		key := string(sstIt.Key())
		if key != expectedOrder[idx] {
			t.Errorf("Position %d: expected %s, got %s", idx, expectedOrder[idx], key)
		}

		if err := sstIt.Next(); err != nil {
			t.Fatalf("Failed to advance iterator: %v", err)
		}
		idx++
	}

	if idx != len(expectedOrder) {
		t.Errorf("Expected %d items, got %d", len(expectedOrder), idx)
	}
}
