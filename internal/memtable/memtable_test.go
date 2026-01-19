package memtable

import (
	"path/filepath"
	"testing"
)

func TestPutGet(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	mt, err := NewMemtable(walPath)
	if err != nil {
		t.Fatalf("Failed to create memtable: %v", err)
	}
	defer mt.Close()

	// Put some data
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		if err := mt.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}

	// Get all data
	for k, expectedV := range testData {
		val, found := mt.Get([]byte(k))
		if !found {
			t.Errorf("Key %s not found", k)
			continue
		}
		if string(val) != expectedV {
			t.Errorf("Key %s: expected %s, got %s", k, expectedV, string(val))
		}
	}

	// Get non-existent key
	_, found := mt.Get([]byte("nonexistent"))
	if found {
		t.Error("Non-existent key should not be found")
	}
}

func TestDelete(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	mt, err := NewMemtable(walPath)
	if err != nil {
		t.Fatalf("Failed to create memtable: %v", err)
	}
	defer mt.Close()

	// Put a key
	if err := mt.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Verify it exists
	val, found := mt.Get([]byte("key1"))
	if !found {
		t.Fatal("Key should exist before delete")
	}
	if string(val) != "value1" {
		t.Errorf("Expected value1, got %s", string(val))
	}

	// Delete it
	if err := mt.Delete([]byte("key1")); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Verify it's gone
	_, found = mt.Get([]byte("key1"))
	if found {
		t.Error("Key should not be found after delete")
	}
}

func TestFreeze(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	mt, err := NewMemtable(walPath)
	if err != nil {
		t.Fatalf("Failed to create memtable: %v", err)
	}
	defer mt.Close()

	// Put some data
	if err := mt.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Freeze
	mt.Freeze()

	// Try to put after freeze
	err = mt.Put([]byte("key2"), []byte("value2"))
	if err != ErrFrozen {
		t.Errorf("Expected ErrFrozen, got %v", err)
	}

	// Try to delete after freeze
	err = mt.Delete([]byte("key1"))
	if err != ErrFrozen {
		t.Errorf("Expected ErrFrozen on delete, got %v", err)
	}

	// Get should still work
	val, found := mt.Get([]byte("key1"))
	if !found {
		t.Error("Get should still work after freeze")
	}
	if string(val) != "value1" {
		t.Errorf("Expected value1, got %s", string(val))
	}
}

func TestRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	// Create first memtable and write data
	mt1, err := NewMemtable(walPath)
	if err != nil {
		t.Fatalf("Failed to create memtable: %v", err)
	}

	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		if err := mt1.Put([]byte(k), []byte(v)); err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}

	mt1.Close()

	// Create new memtable from same WAL (should recover)
	mt2, err := NewMemtable(walPath)
	if err != nil {
		t.Fatalf("Failed to create new memtable: %v", err)
	}
	defer mt2.Close()

	// Verify all data was recovered
	for k, expectedV := range testData {
		val, found := mt2.Get([]byte(k))
		if !found {
			t.Errorf("Key %s not recovered", k)
			continue
		}
		if string(val) != expectedV {
			t.Errorf("Key %s: expected %s, got %s", k, expectedV, string(val))
		}
	}
}

func TestIsFull(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := filepath.Join(tmpDir, "test.wal")

	mt, err := NewMemtable(walPath)
	if err != nil {
		t.Fatalf("Failed to create memtable: %v", err)
	}
	defer mt.Close()

	// Initially should not be full
	if mt.IsFull() {
		t.Error("New memtable should not be full")
	}

	// Put a small value
	if err := mt.Put([]byte("key1"), []byte("value1")); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Still should not be full (unless we put a lot)
	if mt.Size() == 0 {
		t.Error("Size should be non-zero after put")
	}
}
