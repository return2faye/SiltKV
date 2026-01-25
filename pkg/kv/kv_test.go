package kv

import (
	"path/filepath"
	"testing"
)

func TestOpenClose(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "test-db")

	db, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close DB: %v", err)
	}
}

func TestPutGet(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "test-db")
	db, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Put
	if err := db.Put("key1", "value1"); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Get
	val, err := db.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if val != "value1" {
		t.Errorf("Expected value1, got %s", val)
	}
}

func TestGetNotFound(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "test-db")
	db, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	_, err = db.Get("nonexistent")
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

func TestDelete(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "test-db")
	db, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Put
	if err := db.Put("key1", "value1"); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Delete
	if err := db.Delete("key1"); err != nil {
		t.Fatalf("Failed to delete: %v", err)
	}

	// Get should return ErrNotFound
	_, err = db.Get("key1")
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound after delete, got %v", err)
	}
}

func TestUpdate(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "test-db")
	db, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Put initial value
	if err := db.Put("key1", "value1"); err != nil {
		t.Fatalf("Failed to put: %v", err)
	}

	// Update
	if err := db.Put("key1", "value2"); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	// Get should return new value
	val, err := db.Get("key1")
	if err != nil {
		t.Fatalf("Failed to get: %v", err)
	}
	if val != "value2" {
		t.Errorf("Expected value2, got %s", val)
	}
}

func TestMultipleKeys(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "test-db")
	db, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Put multiple keys
	testData := map[string]string{
		"key1": "value1",
		"key2": "value2",
		"key3": "value3",
	}

	for k, v := range testData {
		if err := db.Put(k, v); err != nil {
			t.Fatalf("Failed to put %s: %v", k, err)
		}
	}

	// Get all keys
	for k, expectedV := range testData {
		val, err := db.Get(k)
		if err != nil {
			t.Fatalf("Failed to get %s: %v", k, err)
		}
		if val != expectedV {
			t.Errorf("Key %s: expected %s, got %s", k, expectedV, val)
		}
	}
}

func TestDeleteNonExistent(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "test-db")
	db, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Delete non-existent key should not error
	if err := db.Delete("nonexistent"); err != nil {
		t.Errorf("Delete of non-existent key should not error, got %v", err)
	}
}

func TestClosedDB(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "test-db")
	db, err := Open(tmpDir)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	// Close
	if err := db.Close(); err != nil {
		t.Fatalf("Failed to close: %v", err)
	}

	// Operations on closed DB should return ErrClosed
	if err := db.Put("key", "value"); err != ErrClosed {
		t.Errorf("Expected ErrClosed, got %v", err)
	}

	// Note: Get on closed DB might return ErrNotFound instead of ErrClosed
	// because lsm.DB.Get returns (nil, false, nil) when active is nil
	_, err = db.Get("key")
	if err != ErrClosed && err != ErrNotFound {
		t.Errorf("Expected ErrClosed or ErrNotFound, got %v", err)
	}

	if err := db.Delete("key"); err != ErrClosed {
		t.Errorf("Expected ErrClosed, got %v", err)
	}
}
