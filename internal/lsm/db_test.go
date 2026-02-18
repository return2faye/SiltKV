package lsm

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/return2faye/SiltKV/internal/memtable"
)

// TestWALFileDeletionAfterFlush verifies that WAL files are deleted after successful flush
func TestWALFileDeletionAfterFlush(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "test-db")

	db, err := Open(Options{DataDir: tmpDir})
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Find the initial WAL file (active memtable's WAL)
	// The WAL file should be named "active.wal" initially
	initialWalPath := filepath.Join(tmpDir, "active.wal")

	// Verify WAL file exists initially
	if _, err := os.Stat(initialWalPath); err != nil {
		t.Fatalf("Initial WAL file %s should exist: %v", initialWalPath, err)
	}

	// Write enough data to fill memtable and trigger flush.
	// Use memtable.DefaultMaxSize instead of hardcoding, so the test
	// stays valid even if the memtable size limit changes.
	valueSize := 32 * 1024 // 32KB per value
	entrySize := 2 + valueSize
	// Target total bytes slightly above DefaultMaxSize to guarantee a flush.
	targetBytes := int64(memtable.DefaultMaxSize) * 11 / 10 // 1.1x
	numKeys := int(targetBytes / int64(entrySize))
	if numKeys < 100 {
		numKeys = 100
	}

	flushTriggered := false
	for i := 0; i < numKeys; i++ {
		key := make([]byte, 2)
		key[0] = byte(i >> 8)
		key[1] = byte(i & 0xFF)
		value := make([]byte, valueSize)
		for j := range value {
			value[j] = byte(i + j)
		}
		if err := db.Put(key, value); err != nil {
			t.Fatalf("Failed to put key %d: %v", i, err)
		}
		
		// Check if flush was triggered by looking for new WAL files
		// When flush happens, a new WAL file (active-*.wal) is created
		if !flushTriggered {
			matches, _ := filepath.Glob(filepath.Join(tmpDir, "active-*.wal"))
			if len(matches) > 0 {
				flushTriggered = true
				t.Logf("Flush triggered after writing %d keys", i+1)
				// Check WAL file size to verify we wrote enough data
				if stat, err := os.Stat(initialWalPath); err == nil {
					t.Logf("Initial WAL file size: %d bytes", stat.Size())
				}
			}
		}
	}

	if !flushTriggered {
		// Check WAL file size to see how much data was written
		if stat, err := os.Stat(initialWalPath); err == nil {
			t.Logf("WAL file size after writing %d keys: %d bytes (expected ~%d bytes)", 
				numKeys, stat.Size(), numKeys*(2+valueSize))
		}
		t.Fatalf("Flush was not triggered after writing %d keys. Memtable may not be full.", numKeys)
	}

	// Wait for flush to complete
	// We wait until SSTable file appears, then check WAL deletion
	maxWait := 10 * time.Second
	waitInterval := 200 * time.Millisecond
	waited := 0 * time.Millisecond

	// SSTable path should be initial WAL path with .sst extension
	sstPath := initialWalPath[:len(initialWalPath)-4] + ".sst"

	// Wait for SSTable to be created (indicates flush started/completed)
	sstFound := false
	for waited < maxWait {
		// Check the expected SSTable path
		if _, err := os.Stat(sstPath); err == nil {
			sstFound = true
			break
		}
		// Also check for any .sst files (in case path generation is different)
		matches, _ := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
		if len(matches) > 0 {
			sstFound = true
			sstPath = matches[0] // Use the found SSTable path
			break
		}
		time.Sleep(waitInterval)
		waited += waitInterval
	}

	if !sstFound {
		// List all files in tmpDir for debugging
		files, _ := os.ReadDir(tmpDir)
		t.Logf("Files in tmpDir after flush:")
		for _, f := range files {
			t.Logf("  %s (size: %d)", f.Name(), func() int64 {
				if info, err := f.Info(); err == nil {
					return info.Size()
				}
				return 0
			}())
		}
		t.Fatalf("SSTable file was not created within timeout. Expected: %s", sstPath)
	}

	// Give a bit more time for WAL deletion to complete after SSTable is created
	time.Sleep(500 * time.Millisecond)

	// Verify initial WAL file was deleted after flush
	if _, err := os.Stat(initialWalPath); !os.IsNotExist(err) {
		t.Errorf("Initial WAL file %s should have been deleted after flush, but still exists", initialWalPath)
	}

	// Verify SSTable file exists
	if _, err := os.Stat(sstPath); os.IsNotExist(err) {
		t.Errorf("SSTable file %s should exist after flush, but was not found", sstPath)
	}

	// Verify data can still be read from SSTable
	// Try reading a few keys to ensure data integrity
	testKeys := [][]byte{
		{0x00, 0x00}, // First key
		{byte((numKeys / 2) >> 8), byte((numKeys / 2) & 0xFF)}, // Middle key
		{byte((numKeys - 1) >> 8), byte((numKeys - 1) & 0xFF)}, // Last key
	}

	for _, key := range testKeys {
		val, found, err := db.Get(key)
		if err != nil {
			t.Errorf("Failed to get key %v: %v", key, err)
			continue
		}
		if !found {
			t.Errorf("Key %v not found after flush", key)
			continue
		}
		if len(val) != valueSize {
			t.Errorf("Key %v: expected value size %d, got %d", key, valueSize, len(val))
		}
	}
}

// TestWALFileNotDeletedOnFlushFailure verifies that WAL file is not deleted if flush fails
// This is a defensive test - in current implementation, flush errors are logged but don't prevent deletion
// However, we should verify the behavior
func TestMultipleFlushes(t *testing.T) {
	tmpDir := filepath.Join(t.TempDir(), "test-db")

	db, err := Open(Options{DataDir: tmpDir})
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Trigger multiple flushes by writing data multiple times.
	// Use memtable.DefaultMaxSize to size writes so that each round
	// reliably overflows the memtable at least once.
	valueSize := 16 * 1024 // 16KB per value
	entrySize := 3 + valueSize
	targetBytesPerRound := int64(memtable.DefaultMaxSize) * 6 / 5 // 1.2x
	numKeysPerFlush := int(targetBytesPerRound / int64(entrySize))
	if numKeysPerFlush < 1000 {
		numKeysPerFlush = 1000
	}

	for flushRound := 0; flushRound < 3; flushRound++ {
		// Write data to trigger flush
		for i := 0; i < numKeysPerFlush; i++ {
			key := []byte{byte(flushRound), byte(i >> 8), byte(i & 0xFF)}
			value := make([]byte, valueSize)
			for j := range value {
				value[j] = byte(flushRound + i + j)
			}
			if err := db.Put(key, value); err != nil {
				t.Fatalf("Failed to put key in round %d: %v", flushRound, err)
			}
		}

		// Wait for flush
		time.Sleep(2 * time.Second)
	}

	// After multiple flushes, there should be no WAL files left (all should be deleted)
	// Only the current active memtable's WAL should exist
	walFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.wal"))
	if err != nil {
		t.Fatalf("Failed to glob WAL files: %v", err)
	}

	// Should have at most 1 WAL file (the current active one)
	if len(walFiles) > 1 {
		t.Errorf("Expected at most 1 WAL file after multiple flushes, found %d: %v", len(walFiles), walFiles)
	}

	// Verify SSTable files exist
	sstFiles, err := filepath.Glob(filepath.Join(tmpDir, "*.sst"))
	if err != nil {
		t.Fatalf("Failed to glob SSTable files: %v", err)
	}

	if len(sstFiles) == 0 {
		t.Error("Expected at least one SSTable file after flushes, but none found")
	}
}
